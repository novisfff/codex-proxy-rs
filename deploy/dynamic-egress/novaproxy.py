"""NovaProxy Rotating：每个租约只建立一个上游 CONNECT，不探测或猜测出口 IP。"""

import asyncio
import base64
import contextlib
import json
import os
import re
from pathlib import Path

import h11


class NovaProxy:
    @staticmethod
    def validate(config):
        host = config.get("host")
        if not isinstance(host, str) or not re.fullmatch(r"[a-zA-Z0-9-]+\.novaproxy\.io", host):
            raise ValueError("Invalid NovaProxy gateway")
        if type(config.get("port")) is not int or not 1 <= config["port"] <= 65535:
            raise ValueError("Invalid proxy port")
        for field in ("username", "password"):
            value = config.get(field)
            if not isinstance(value, str) or not 1 <= len(value) <= 1024 or any(ord(c) < 33 or ord(c) > 126 for c in value):
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
        # 只接受供应商域名，防止误填第三方地址时泄露认证信息。
        self.validate(credentials)
        reader, writer = await asyncio.open_connection(credentials["host"], credentials["port"])
        try:
            connection = h11.Connection(h11.CLIENT, max_incomplete_event_size=16384)
            auth = base64.b64encode(f"{credentials['username']}:{credentials['password']}".encode())
            writer.write(connection.send(h11.Request(method=b"CONNECT", target=target,
                headers=[(b"host", target), (b"proxy-authorization", b"Basic " + auth)])))
            writer.write(connection.send(h11.EndOfMessage()))
            await writer.drain()
            while True:
                event = connection.next_event()
                if event is h11.NEED_DATA:
                    data = await reader.read(8192)
                    if not data:
                        raise ConnectionError("Proxy closed before CONNECT response")
                    connection.receive_data(data)
                elif isinstance(event, h11.InformationalResponse):
                    continue
                elif isinstance(event, h11.Response) and event.status_code == 200:
                    return reader, writer, connection.trailing_data[0]
                else:
                    # 不透传供应商错误正文或认证上下文。
                    raise ConnectionError("NovaProxy CONNECT rejected")
        except BaseException:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            raise
