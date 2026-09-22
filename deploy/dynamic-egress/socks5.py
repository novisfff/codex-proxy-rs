"""通用 SOCKS5 代理：每个租约只建立一个上游 SOCKS5 CONNECT，不探测或猜测出口 IP。"""

import asyncio
import contextlib
import ipaddress
import json
import os
import re
import secrets
import string
from urllib.parse import quote
from pathlib import Path


SESSION_MARKER = "__CPR_292_SID__"
_SESSION_ALPHABET = string.ascii_lowercase + string.digits
_SESSION_LENGTH = 12


class Socks5Proxy:
    @classmethod
    def prepare_credentials(cls, config):
        credentials = dict(config)
        if any(SESSION_MARKER in credentials[field] for field in ("username", "password")):
            sid = "".join(secrets.choice(_SESSION_ALPHABET) for _ in range(_SESSION_LENGTH))
            # 同一租约只生成一次 SID，连接和回传地址共用替换后的凭据。
            for field in ("username", "password"):
                credentials[field] = credentials[field].replace(SESSION_MARKER, sid)

        host = credentials["host"]
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            host = host.encode("idna").decode("ascii").removesuffix(".")
        else:
            if address.version == 6:
                host = f"[{host}]"
        proxy_url = (
            f"socks5h://{quote(credentials['username'], safe='')}:"
            f"{quote(credentials['password'], safe='')}@{host}:{credentials['port']}"
        )
        return credentials, proxy_url

    @staticmethod
    def validate(config):
        host = config.get("host")
        if not isinstance(host, str) or not host or host != host.strip():
            raise ValueError("Invalid SOCKS5 host")
        try:
            ipaddress.ip_address(host)
            if "%" in host:
                raise ValueError("Invalid SOCKS5 host")
        except ValueError:
            # 地址和端口分开配置；只校验主机语法，不限制供应商或网络范围。
            try:
                domain = host.encode("idna").decode("ascii").removesuffix(".")
            except UnicodeError:
                raise ValueError("Invalid SOCKS5 host") from None
            if (len(domain) > 253 or not all(
                    re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
                    for label in domain.split("."))):
                raise ValueError("Invalid SOCKS5 host")
        if type(config.get("port")) is not int or not 1 <= config["port"] <= 65535:
            raise ValueError("Invalid proxy port")
        for field in ("username", "password"):
            value = config.get(field)
            # RFC 1929 的用户名和密码长度字段各占一个字节；代理凭据只允许
            # 可打印 ASCII，避免 URL/握手编码产生歧义。
            if (not isinstance(value, str) or not 1 <= len(value) <= 255
                    or any(ord(c) < 33 or ord(c) > 126 for c in value)):
                raise ValueError("Invalid credentials")
        if ":" in config["username"]:
            raise ValueError("Invalid username")

    def credentials(self, reference):
        # 仅用于迁移旧安装；新配置直接保存在数据库中。
        if not isinstance(reference, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", reference):
            raise ValueError("Invalid credential reference")
        directory = Path(os.environ.get("EGRESS_NOVAPROXY_CREDENTIALS_DIR", "/etc/cpr292-egress/novaproxy"))
        path = directory / f"{reference}.json"
        if path.is_symlink() or path.stat().st_mode & 0o007:
            raise ValueError("Unsafe credential permissions")
        data = json.loads(path.read_text())
        if not isinstance(data, dict) or set(data) != {"username", "password"}:
            raise ValueError("Invalid credentials")
        self.validate(dict(data, host="residential-gateway.novaproxy.io", port=1111))
        return data

    async def connect(self, credentials, target):
        self.validate(credentials)
        reader, writer = await asyncio.open_connection(credentials["host"], credentials["port"])
        try:
            host, port = _target_parts(target)
            username = credentials["username"].encode("ascii")
            password = credentials["password"].encode("ascii")

            # 代理网关使用 RFC 1928 的 SOCKS5 握手和 RFC 1929 用户名/密码认证。
            writer.write(b"\x05\x01\x02")
            await writer.drain()
            version, method = await _read_exactly(reader, 2)
            if version != 5 or method != 2:
                raise ConnectionError("SOCKS5 authentication rejected")

            writer.write(
                b"\x01" + bytes((len(username),)) + username + bytes((len(password),)) + password
            )
            await writer.drain()
            auth_version, auth_status = await _read_exactly(reader, 2)
            if auth_version != 1 or auth_status != 0:
                raise ConnectionError("SOCKS5 authentication rejected")

            address_type, address = _socks5_address(host)
            writer.write(b"\x05\x01\x00" + address_type + address + port.to_bytes(2, "big"))
            await writer.drain()
            response = await _read_exactly(reader, 4)
            response_version, reply, reserved, response_type = response
            if response_version != 5 or reserved != 0:
                raise ConnectionError("SOCKS5 handshake failed")
            await _discard_bound_address(reader, response_type)
            await _read_exactly(reader, 2)  # BND.PORT
            if reply != 0:
                # 不透传供应商错误正文或认证上下文。
                raise ConnectionError("SOCKS5 CONNECT rejected")
            return reader, writer, b""
        except BaseException:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            raise


async def _read_exactly(reader, size):
    try:
        return await reader.readexactly(size)
    except (asyncio.IncompleteReadError, ConnectionError) as error:
        raise ConnectionError("SOCKS5 handshake failed") from error


def _target_parts(target):
    try:
        value = target.decode("ascii")
    except UnicodeDecodeError as error:
        raise ConnectionError("Invalid SOCKS5 target") from error
    if value.startswith("["):
        host, separator, port_text = value[1:].partition("]:")
    else:
        host, separator, port_text = value.rpartition(":")
    if not separator or not host or not port_text:
        raise ConnectionError("Invalid SOCKS5 target")
    try:
        port = int(port_text)
    except ValueError as error:
        raise ConnectionError("Invalid SOCKS5 target") from error
    if not 1 <= port <= 65535:
        raise ConnectionError("Invalid SOCKS5 target")
    return host, port


def _socks5_address(host):
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        encoded = host.encode("idna")
        if not 1 <= len(encoded) <= 255:
            raise ConnectionError("Invalid SOCKS5 target")
        return b"\x03", bytes((len(encoded),)) + encoded
    if address.version == 4:
        return b"\x01", address.packed
    return b"\x04", address.packed


async def _discard_bound_address(reader, address_type):
    if address_type == 1:
        await _read_exactly(reader, 4)
    elif address_type == 3:
        length = (await _read_exactly(reader, 1))[0]
        await _read_exactly(reader, length)
    elif address_type == 4:
        await _read_exactly(reader, 16)
    else:
        raise ConnectionError("SOCKS5 handshake failed")
