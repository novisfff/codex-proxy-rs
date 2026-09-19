"""292 获取器专用出口。控制面不接收 OpenAI 凭据，隧道不解密 TLS。"""

import asyncio
import base64
import contextlib
import fcntl
import hmac
import ipaddress
import json
import os
import re
import secrets
import signal
import socket
import sqlite3
import time
from pathlib import Path

import aiohttp
from aiohttp import web
import h11
from novaproxy import NovaProxy


class EgressError(Exception):
    pass


class Journal:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS history(ip TEXT PRIMARY KEY, used REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, instance TEXT NOT NULL, family TEXT NOT NULL,
                state TEXT NOT NULL, ip TEXT, created REAL NOT NULL,
                expires REAL, secret TEXT, message TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS resources(
                name TEXT PRIMARY KEY, instance TEXT NOT NULL, family TEXT NOT NULL,
                confirmed INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS settings(id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL, revision INTEGER NOT NULL);
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(resources)")}
        if "confirmed" not in columns:
            with self.db:
                self.db.execute("ALTER TABLE resources ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0")
        settings_columns = {row[1] for row in self.db.execute("PRAGMA table_info(settings)")}
        if settings_columns and "revision" not in settings_columns:
            with self.db:
                self.db.execute("ALTER TABLE settings ADD COLUMN revision INTEGER NOT NULL DEFAULT 0")
        if "provider" not in {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}:
            with self.db:
                self.db.execute("ALTER TABLE jobs ADD COLUMN provider TEXT NOT NULL DEFAULT 'azure'")

    def execute(self, sql, args=()):
        with self.db:
            return self.db.execute(sql, args)

    def reserve(self, address, now):
        address = str(ipaddress.ip_address(address))
        with self.db:
            row = self.db.execute("SELECT used FROM history WHERE ip=?", (address,)).fetchone()
            if row and row[0] > now - 86400:
                return False
            self.db.execute("INSERT OR REPLACE INTO history VALUES(?,?)", (address, now))
        return True

    def used(self, address):
        # 失败、探测和连接结束也计入窗口，避免长请求缩短隔离时间。
        self.execute("UPDATE history SET used=max(used,?) WHERE ip=?", (time.time(), address))

    def job(self, job_id):
        row = self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None


class Azure:
    def __init__(self, instances, journal, owner):
        self.instances, self.journal, self.owner = instances, journal, owner
        self.logged_in = False

    async def command(self, *args):
        process = await asyncio.create_subprocess_exec(
            "az", *args, "--only-show-errors", "--output", "json",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), 600)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise
        if process.returncode:
            # CLI 错误正文可能包含认证上下文，不进入控制面。
            operation = " ".join(args[:3]) if args[0] == "network" else "managed identity login"
            raise EgressError(f"Azure {operation} failed; inspect managed identity, quota and NIC configuration")
        return json.loads(stdout) if stdout.strip() else None

    async def start(self):
        if not self.logged_in and (any(c["provider"] == "azure" for c in self.instances.values())
                                   or self.journal.db.execute("SELECT count(*) FROM resources").fetchone()[0]):
            await self.command("login", "--identity")
            self.logged_in = True
        await self.recover()

    def binding(self, instance, family):
        config = self.instances[instance]
        if config.get("provider") != "azure" or family not in config.get("bindings", {}):
            raise EgressError("Unsupported provider or IP family")
        return config, config["bindings"][family]

    async def nic(self, config, binding):
        return await self.command("network", "nic", "show", "--subscription", config["subscription"],
                                  "--resource-group", binding["resourceGroup"], "--name", binding["nic"])

    async def preflight(self, instance, family):
        config, binding = self.binding(instance, family)
        nic = await self.nic(config, binding)
        entries = nic.get("ipConfigurations", [])
        target = next((p for p in entries if p["name"] == binding["ipConfiguration"]), None)
        if (not target or target.get("privateIPAddressVersion", "IPv4").lower() != family
                or str(ipaddress.ip_address(target["privateIPAddress"])) != binding["sourceIp"]
                or not binding.get("dedicated")
                or (family == "ipv4" and target.get("primary"))
                or not any(p.get("privateIPAddressVersion", "IPv4") == "IPv4" for p in entries)):
            raise EgressError("Dedicated NIC IP configuration preflight failed")
        if target.get("publicIPAddress"):
            raise EgressError("Dedicated IP configuration must be detached before allocation")
        if nic.get("provisioningState") != "Succeeded":
            raise EgressError("NIC is not ready")
        if family == "ipv6":
            await self.check_ipv6_source(binding["sourceIp"])
        # 仅验证操作系统已经配置好私网源地址，绝不修改默认路由和主网卡。
        with socket.socket(socket.AF_INET if family == "ipv4" else socket.AF_INET6) as sock:
            sock.bind((binding["sourceIp"], 0))
        return config, binding

    async def check_ipv6_source(self, source):
        process = await asyncio.create_subprocess_exec(
            "ip", "-j", "-6", "address", "show", stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL)
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 5)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise
        addresses = [address for interface in json.loads(output) for address in interface.get("addr_info", [])] if process.returncode == 0 else []
        # deprecated 地址仍可显式绑定，但普通连接不会优先自动选择这个动态出口。
        if not any(address.get("local") == source and address.get("preferred_life_time") == 0 for address in addresses):
            raise EgressError("Dedicated IPv6 source must have preferred_lft 0 to exclude ordinary traffic")

    async def allocate(self, instance, family):
        await self.start()
        config, binding = await self.preflight(instance, family)
        await self.cleanup_unused_public_ips(config)
        for _ in range(5):
            name = f"cpr292-{secrets.token_hex(12)}"
            # 先记录确定性资源名；进程崩溃或 ARM 超时后可对账，不能盲目重放创建。
            self.journal.execute("INSERT INTO resources(name,instance,family) VALUES(?,?,?)", (name, instance, family))
            result = await self.command(
                "network", "public-ip", "create", "--subscription", config["subscription"],
                "--resource-group", config["resourceGroup"], "--name", name,
                "--location", config["location"], "--sku", "Standard",
                "--allocation-method", "Static", "--version", "IPv4" if family == "ipv4" else "IPv6",
                "--tags", f"cpr292-owner={self.owner}")
            public = result["publicIp"]
            self.journal.execute("UPDATE resources SET confirmed=1 WHERE name=?", (name,))
            address = str(ipaddress.ip_address(public["ipAddress"]))
            if ipaddress.ip_address(address).version != (4 if family == "ipv4" else 6):
                raise EgressError("Azure returned an unexpected address family")
            if not self.journal.reserve(address, time.time()):
                await self.cleanup(name, instance, family)
                continue
            await self.command(
                "network", "nic", "ip-config", "update", "--subscription", config["subscription"],
                "--resource-group", binding["resourceGroup"], "--nic-name", binding["nic"],
                "--name", binding["ipConfiguration"], "--public-ip-address", public["id"])
            await self.verify(binding["sourceIp"], family, address)
            return address, binding["sourceIp"]
        raise EgressError("Azure repeatedly allocated IPs already used within 24 hours")

    async def cleanup_unused_public_ips(self, config):
        # 此资源组专供获取器使用；先保护所有配置网卡的主 IPv4，再清理闲置公网地址。
        protected = set()
        for binding in config["bindings"].values():
            nic = await self.nic(config, binding)
            primary = next((p for p in nic.get("ipConfigurations", [])
                            if p.get("primary") and p.get("privateIPAddressVersion", "IPv4") == "IPv4"), None)
            public_id = (primary.get("publicIPAddress") or {}).get("id") if primary else None
            if not public_id:
                raise EgressError("Cannot identify primary IPv4 public IP; stale cleanup stopped")
            protected.add(public_id.lower())
        publics = await self.command("network", "public-ip", "list", "--subscription", config["subscription"],
                                     "--resource-group", config["resourceGroup"])
        candidates = [p for p in publics if p["id"].lower() not in protected]
        # 先完整检查再删除，遇到其他业务绑定时不自动解绑。
        if any(p.get("ipConfiguration") or p.get("natGateway") or p.get("linkedPublicIPAddress")
               or p.get("servicePublicIPAddress") or p.get("provisioningState") != "Succeeded"
               for p in candidates):
            raise EgressError("Non-primary public IP is attached or not ready; stale cleanup stopped")
        for public in candidates:
            if public.get("ipAddress"):
                self.journal.reserve(public["ipAddress"], time.time())
                self.journal.used(public["ipAddress"])
            await self.command("network", "public-ip", "delete", "--subscription", config["subscription"],
                               "--resource-group", config["resourceGroup"], "--name", public["name"])

    async def verify(self, source, family, expected):
        connector = aiohttp.TCPConnector(
            local_addr=(source, 0), family=socket.AF_INET if family == "ipv4" else socket.AF_INET6)
        async with aiohttp.ClientSession(connector=connector, trust_env=False,
                                        timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get("https://api64.ipify.org", allow_redirects=False) as response:
                body = await response.content.read(128)
                if response.status != 200 or str(ipaddress.ip_address(body.decode().strip())) != expected:
                    raise EgressError("Actual outbound IP does not match allocated IP")

    async def cleanup(self, name, instance, family):
        config, binding = self.binding(instance, family)
        # list 可明确区分不存在与查询失败；失败不得当作已经删除。
        publics = await self.command("network", "public-ip", "list", "--subscription", config["subscription"],
                                     "--resource-group", config["resourceGroup"])
        public = next((p for p in publics if p["name"] == name), None)
        record = self.journal.db.execute("SELECT confirmed FROM resources WHERE name=?", (name,)).fetchone()
        if not public and record and not record[0]:
            # ARM 操作可能在 CLI 超时后继续执行；没有看到资源不等于创建已取消。
            raise EgressError("Uncertain Azure creation; reconciliation must confirm the final ARM result")
        if public:
            if public.get("tags", {}).get("cpr292-owner") != self.owner:
                raise EgressError("Resource ownership mismatch; cleanup stopped")
            if public.get("ipAddress"):
                self.journal.used(str(ipaddress.ip_address(public["ipAddress"])))
            # 等待任何先前提交的 NIC 操作结束后再决定是否解除绑定。
            await self.command("network", "nic", "wait", "--subscription", config["subscription"],
                               "--resource-group", binding["resourceGroup"], "--name", binding["nic"],
                               "--updated", "--timeout", "600", "--interval", "5")
            nic = await self.nic(config, binding)
            target = next(p for p in nic["ipConfigurations"] if p["name"] == binding["ipConfiguration"])
            association = target.get("publicIPAddress") or {}
            if association.get("id", "").lower() == public["id"].lower():
                await self.command("network", "nic", "ip-config", "update", "--subscription", config["subscription"],
                                   "--resource-group", binding["resourceGroup"], "--nic-name", binding["nic"],
                                   "--name", binding["ipConfiguration"], "--remove", "publicIPAddress")
            elif public.get("ipConfiguration"):
                raise EgressError("Public IP is attached outside the dedicated configuration")
            await self.command("network", "public-ip", "delete", "--subscription", config["subscription"],
                               "--resource-group", config["resourceGroup"], "--name", name)
        self.journal.execute("DELETE FROM resources WHERE name=?", (name,))

    async def recover(self):
        for row in self.journal.db.execute("SELECT * FROM resources").fetchall():
            await self.cleanup(row["name"], row["instance"], row["family"])


class Service:
    def __init__(self, config, journal, provider):
        self.config, self.journal, self.provider = config, journal, provider
        self.novaproxy = NovaProxy()
        self.nova_credentials = None
        self.active = None
        self.task = None
        self.ready = False
        self.fault = ""
        self.tunnel = None
        self.source = None
        self.released = asyncio.Event()
        saved = self.journal.db.execute("SELECT data,revision FROM settings WHERE id=1").fetchone()
        self.revision = saved[1] if saved else 0
        if saved:
            self.config["instances"] = json.loads(saved[0])
            self.provider.instances = self.config["instances"]

    async def start(self):
        try:
            await self.provider.start()
            self.journal.execute("UPDATE jobs SET state='released',secret=NULL WHERE state NOT IN ('released','failed')")
            self.ready = True
            self.journal.execute("DELETE FROM jobs WHERE created<? AND state IN ('released','failed')", (time.time() - 7 * 86400,))
        except Exception:
            self.fault = "Startup reconciliation failed; inspect Azure resources before restarting"

    def snapshot(self):
        instances = [{"id": key, **c, "families": list(c["bindings"])}
                     for key, c in self.config["instances"].items()]
        history = [dict(row) for row in self.journal.db.execute(
            "SELECT id,instance,family,state,ip,created,expires,message,provider FROM jobs ORDER BY created DESC LIMIT 50")]
        return {"available": self.ready and not self.fault, "message": self.fault,
                "instances": instances, "history": history, "revision": self.revision}

    def configure(self, instances, revision):
        if self.active or self.fault or not self.ready or type(revision) is not int or revision != self.revision:
            raise web.HTTPConflict()
        validate_instances(instances)
        self.journal.execute("INSERT OR REPLACE INTO settings VALUES(1,?,?)", (json.dumps(instances), self.revision + 1))
        self.revision += 1
        self.config["instances"] = instances
        self.provider.instances = instances

    async def acquire(self, job_id, instance, family):
        if not re.fullmatch(r"[a-f0-9-]{36}", job_id):
            raise web.HTTPBadRequest()
        existing = self.journal.job(job_id)
        if existing:
            if (existing["instance"], existing["family"]) != (instance, family):
                raise web.HTTPConflict()
            return self.present(existing)
        if not self.ready or self.fault:
            raise web.HTTPServiceUnavailable()
        config = self.config["instances"].get(instance)
        if not config or family not in config["bindings"]:
            raise web.HTTPBadRequest()
        if self.active:
            raise web.HTTPConflict()
        self.journal.execute("DELETE FROM jobs WHERE created<? AND state IN ('released','failed')", (time.time() - 7 * 86400,))
        self.active = job_id
        self.released = asyncio.Event()
        self.journal.execute("INSERT INTO jobs(id,instance,family,state,created,provider) VALUES(?,?,?,'provisioning',?,?)",
                             (job_id, instance, family, time.time(), config["provider"]))
        self.task = asyncio.create_task(self.run(job_id, instance, family))
        return self.present(self.journal.job(job_id))

    def present(self, row):
        result = {k: row[k] for k in ("id", "state", "ip", "message")}
        result.update(provider=row["provider"], ipVerification="unverified" if row["provider"] == "novaproxy" else "verified")
        if row["state"] == "ready":
            result.update(proxyUrl=self.config["proxyUrl"], secret=row["secret"])
        return result

    async def run(self, job_id, instance, family):
        try:
            if self.config["instances"][instance]["provider"] == "novaproxy":
                self.nova_credentials = self.novaproxy.credentials(self.config["instances"][instance]["credentialRef"])
                address, self.source = None, None
            else:
                address, self.source = await self.provider.allocate(instance, family)
            if not self.released.is_set():
                self.journal.execute("UPDATE jobs SET state='ready',ip=?,secret=?,expires=? WHERE id=?",
                                     (address, secrets.token_urlsafe(32), time.time() + 90, job_id))
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.released.wait(), 90)
        except Exception as error:
            message = str(error) if isinstance(error, EgressError) else "Address allocation or verification failed"
            self.journal.execute("UPDATE jobs SET state='failed',message=? WHERE id=?", (message, job_id))
        finally:
            if self.tunnel:
                self.tunnel.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self.tunnel
            row = self.journal.job(job_id)
            if row["ip"]:
                self.journal.used(row["ip"])
            try:
                await self.provider.recover()
            except Exception:
                self.fault = "Azure cleanup is incomplete; new leases blocked until reconciliation succeeds"
            self.journal.execute("UPDATE jobs SET state=CASE WHEN state='failed' THEN state ELSE 'released' END,secret=NULL WHERE id=?", (job_id,))
            self.source, self.active, self.tunnel = None, None, None
            self.nova_credentials = None

    async def release(self, job_id):
        if not re.fullmatch(r"[a-f0-9-]{36}", job_id):
            raise web.HTTPBadRequest()
        if self.active == job_id:
            self.released.set()
        elif not self.journal.job(job_id):
            # 取消先于迟到的 acquire 到达时，用墓碑阻止该 ID 启动资源分配。
            self.journal.execute("INSERT INTO jobs(id,instance,family,state,created) VALUES(?,'','','released',?)", (job_id, time.time()))

    async def connect(self, reader, writer):
        connection = h11.Connection(h11.SERVER, max_incomplete_event_size=16384)
        accepted = False
        upstream_writer = None
        try:
            async with asyncio.timeout(15):
                while True:
                    event = connection.next_event()
                    if event is h11.NEED_DATA:
                        data = await reader.read(8192)
                        if not data:
                            return
                        connection.receive_data(data)
                    elif isinstance(event, h11.Request):
                        break
                    else:
                        raise EgressError("Invalid CONNECT")
                headers = dict(event.headers)
                row = self.journal.job(self.active) if self.active else None
                expected = b"Basic " + base64.b64encode(f"{self.active}:{row['secret']}".encode()) if row else b""
                if (event.method != b"CONNECT" or event.target not in (b"chatgpt.com:443", b"api.openai.com:443")
                        or not row or row["state"] != "ready" or row["expires"] <= time.time()
                        or self.released.is_set()
                        or not hmac.compare_digest(headers.get(b"proxy-authorization", b""), expected)):
                    raise EgressError("CONNECT denied")
                # await 之前原子消耗租约，一次连接失败也不能再次使用同一 IP。
                self.journal.execute("UPDATE jobs SET state='connected' WHERE id=?", (self.active,))
                if row["ip"]:
                    self.journal.used(row["ip"])
                self.tunnel = asyncio.current_task()
                accepted = True
                proxy_trailing = b""
                if row["provider"] == "novaproxy":
                    upstream_reader, upstream_writer, proxy_trailing = await self.novaproxy.connect(self.nova_credentials, event.target)
                else:
                    family = socket.AF_INET if row["family"] == "ipv4" else socket.AF_INET6
                    host = event.target.decode().split(":")[0]
                    addresses = await asyncio.get_running_loop().getaddrinfo(host, 443, family=family, type=socket.SOCK_STREAM)
                    address = addresses[0][4][0]
                    if not ipaddress.ip_address(address).is_global:
                        raise EgressError("Non-public destination")
                    upstream_reader, upstream_writer = await asyncio.open_connection(
                        address, 443, family=family, local_addr=(self.source, 0))
                writer.write(connection.send(h11.Response(status_code=200, headers=[])))
                writer.write(proxy_trailing)
                await writer.drain()
                # CONNECT 切换后剩余字节属于 TLS，不可丢弃。
                trailing, _ = connection.trailing_data
                if trailing:
                    upstream_writer.write(trailing)
                    await upstream_writer.drain()
            async with asyncio.timeout(60):
                async def pump(source, target):
                    while chunk := await source.read(65536):
                        target.write(chunk)
                        await target.drain()
                tasks = [asyncio.create_task(pump(reader, upstream_writer)),
                         asyncio.create_task(pump(upstream_reader, writer))]
                try:
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            if not accepted:
                with contextlib.suppress(Exception):
                    writer.write(connection.send(h11.Response(status_code=403, headers=[(b"content-length", b"0")])))
                    await writer.drain()
        finally:
            for stream in (upstream_writer, writer):
                if stream:
                    stream.close()
                    with contextlib.suppress(Exception):
                        await stream.wait_closed()
            if accepted:
                self.tunnel = None
                self.released.set()


def application(service, token):
    @web.middleware
    async def authenticate(request, handler):
        if not hmac.compare_digest(request.headers.get("Authorization", ""), f"Bearer {token}"):
            raise web.HTTPUnauthorized()
        return await handler(request)

    app = web.Application(middlewares=[authenticate], client_max_size=32768)

    async def status(_):
        return web.json_response(service.snapshot())

    async def acquire(request):
        body = await request.json()
        if not isinstance(body, dict) or set(body) != {"id", "instance", "family"} or not all(isinstance(v, str) for v in body.values()):
            raise web.HTTPBadRequest()
        return web.json_response(await service.acquire(body["id"], body["instance"], body["family"]))

    async def release(request):
        await service.release(request.match_info["id"])
        return web.json_response({})

    async def configure(request):
        body = await request.json()
        if not isinstance(body, dict) or set(body) != {"instances", "revision"}:
            raise web.HTTPBadRequest()
        service.configure(body["instances"], body["revision"])
        return web.json_response({})

    app.router.add_get("/v1/status", status)
    app.router.add_post("/v1/leases", acquire)
    app.router.add_delete("/v1/leases/{id}", release)
    app.router.add_put("/v1/instances", configure)
    return app


def validate_instances(instances):
    if not isinstance(instances, dict) or len(instances) > 16:
        raise web.HTTPBadRequest()
    seen = set()
    for key, config in instances.items():
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", key) or not isinstance(config, dict):
            raise web.HTTPBadRequest()
        if config.get("provider") == "novaproxy":
            if (set(config) != {"provider", "name", "credentialRef", "bindings"}
                    or not isinstance(config["name"], str) or not 1 <= len(config["name"]) <= 128
                    or any(ord(c) < 32 for c in config["name"])
                    or not isinstance(config["credentialRef"], str)
                    or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", config["credentialRef"])
                    or config["bindings"] != {"ipv4": {}}):
                raise web.HTTPBadRequest()
            continue
        if set(config) != {"provider", "name", "subscription", "resourceGroup", "location", "bindings"}:
            raise web.HTTPBadRequest()
        if config["provider"] != "azure" or not isinstance(config["bindings"], dict) or not config["bindings"]:
            raise web.HTTPBadRequest()
        for field in ("name", "subscription", "resourceGroup", "location"):
            if not isinstance(config[field], str) or not re.fullmatch(r"[\w.() -]{1,128}", config[field]) or config[field].startswith("-"):
                raise web.HTTPBadRequest()
        for family, binding in config["bindings"].items():
            if family not in ("ipv4", "ipv6") or not isinstance(binding, dict):
                raise web.HTTPBadRequest()
            if set(binding) != {"resourceGroup", "nic", "ipConfiguration", "sourceIp", "dedicated"} or binding["dedicated"] is not True:
                raise web.HTTPBadRequest()
            for field in ("resourceGroup", "nic", "ipConfiguration"):
                if not isinstance(binding[field], str) or not re.fullmatch(r"[\w.()-]{1,128}", binding[field]) or binding[field].startswith("-"):
                    raise web.HTTPBadRequest()
            try:
                address = ipaddress.ip_address(binding["sourceIp"])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest() from None
            if address.version != (4 if family == "ipv4" else 6) or address.is_loopback or address.is_unspecified or address.is_multicast:
                raise web.HTTPBadRequest()
            binding["sourceIp"] = str(address)
            identity = (config["subscription"], binding["resourceGroup"], binding["nic"], binding["ipConfiguration"])
            if identity in seen:
                raise web.HTTPBadRequest()
            seen.add(identity)


async def main():
    os.umask(0o077)
    config = json.loads(Path(os.environ["EGRESS_CONFIG"]).read_text())
    validate_instances(config["instances"])
    token = Path(os.environ["EGRESS_TOKEN_FILE"]).read_text().strip()
    if len(token) < 32:
        raise ValueError("Control token must contain at least 32 characters")
    lock = open(config["database"] + ".lock", "a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    journal = Journal(config["database"])
    provider = Azure(config["instances"], journal, config["owner"])
    service = Service(config, journal, provider)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stopped.set)
    runner = web.AppRunner(application(service, token), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, config["listen"], config["controlPort"]).start()
    proxy = await asyncio.start_server(service.connect, config["listen"], config["proxyPort"])
    await service.start()
    try:
        async with proxy:
            await stopped.wait()
    finally:
        service.ready = False
        if service.active:
            await service.release(service.active)
        if service.task:
            await asyncio.shield(service.task)
        await runner.cleanup()
        journal.db.close()


if __name__ == "__main__":
    asyncio.run(main())
