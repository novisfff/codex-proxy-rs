import asyncio
import base64
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp import web
from egress import Service, validate_instances
from novaproxy import NovaProxy
import test_egress


def instance():
    return {"provider": "novaproxy", "name": "Nova test", "host": "residential-gateway.novaproxy.io", "port": 1111,
            "username": "test-country-us", "password": "private-password", "bindings": {"ipv4": {}}}


class NovaTests(unittest.IsolatedAsyncioTestCase):
    def test_configuration_rejects_ipv6_credentials_and_paths(self):
        validate_instances({"nova": instance()})
        for change in ({"bindings": {"ipv6": {}}}, {"password": ""}, {"credentialRef": "../secret"},
                       {"host": "novaproxy.io.evil.example"}, {"port": True}, {"port": 0},
                       {"username": "bad\r\nuser"}, {"password": "x" * 256}):
            with self.assertRaises(web.HTTPBadRequest):
                validate_instances({"nova": dict(instance(), **change)})

    def test_credentials_permissions_and_validation(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, EGRESS_NOVAPROXY_CREDENTIALS_DIR=directory):
            path = Path(directory) / "test-us.json"
            path.write_text(json.dumps({"username": "test-country-us", "password": "test-password"}))
            path.chmod(0o600)
            self.assertEqual(NovaProxy().credentials("test-us")["username"], "test-country-us")
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                NovaProxy().credentials("test-us")
            with self.assertRaises(ValueError):
                NovaProxy().credentials("../test-us")

    async def test_connect_auth_tunnel_and_rejection(self):
        requests = []
        async def gateway(reader, writer):
            try:
                self.assertEqual(await reader.readexactly(3), b"\x05\x01\x02")
                writer.write(b"\x05\x02")
                await writer.drain()
                self.assertEqual(await reader.readexactly(1), b"\x01")
                username = await reader.readexactly((await reader.readexactly(1))[0])
                password_length = (await reader.readexactly(1))[0]
                password = await reader.readexactly(password_length)
                writer.write(b"\x01\x00")
                await writer.drain()
                # 第一次请求成功，第二次验证 SOCKS5 CONNECT 被供应商拒绝时的处理。
                request = await reader.readexactly(4)
                self.assertEqual(request, b"\x05\x01\x00\x03")
                host_length = (await reader.readexactly(1))[0]
                host = await reader.readexactly(host_length)
                port = await reader.readexactly(2)
                requests.append((username, password, host, port))
                if len(requests) == 1:
                    writer.write(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x00hello")
                else:
                    writer.write(b"\x05\x02\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(gateway, "127.0.0.1", 0)
        original = asyncio.open_connection
        async def dial(host, port):
            self.assertEqual((host, port), ("residential-gateway.novaproxy.io", 1111))
            return await original("127.0.0.1", server.sockets[0].getsockname()[1])
        try:
            with patch("novaproxy.asyncio.open_connection", dial):
                reader, writer, trailing = await NovaProxy().connect(dict(instance(), username="test", password="secret"), b"api.openai.com:443")
                self.assertEqual(trailing + await reader.read(), b"hello")
                writer.close()
                await writer.wait_closed()
                self.assertEqual(requests[0], (b"test", b"secret", b"api.openai.com", b"\x01\xbb"))
                with self.assertRaisesRegex(ConnectionError, "^NovaProxy SOCKS5 CONNECT rejected$"):
                    await NovaProxy().connect(dict(instance(), username="test", password="secret"), b"api.openai.com:443")
                self.assertEqual(len(requests), 2)
        finally:
            server.close()
            await server.wait_closed()


class NovaServiceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_egress.ServiceTests.asyncSetUp
    asyncTearDown = test_egress.ServiceTests.asyncTearDown
    ready = test_egress.ServiceTests.ready

    async def test_concurrency_limit_and_independent_release(self):
        self.service.configure({"nova": dict(instance(), maxConcurrent=2, intervalSeconds=0)}, 0)
        second = "22222222-2222-4222-8222-222222222222"
        third = "33333333-3333-4333-8333-333333333333"
        await self.service.acquire(self.id, "nova", "ipv4")
        await self.service.acquire(second, "nova", "ipv4")
        await self.ready()
        with self.assertRaises(web.HTTPConflict):
            await self.service.acquire(third, "nova", "ipv4")
        await self.service.release(self.id)
        await self.service.jobs[self.id]["task"]
        self.assertEqual(self.journal.job(second)["state"], "ready")
        await self.service.acquire(third, "nova", "ipv4")
        self.assertEqual(len(self.service.jobs), 2)
        self.assertEqual(self.provider.cleanups, 0)

    async def test_interval_is_between_starts_and_idempotent_acquire_does_not_consume_slot(self):
        self.service.configure({"nova": dict(instance(), maxConcurrent=2, intervalSeconds=10)}, 0)
        second = "22222222-2222-4222-8222-222222222222"
        await self.service.acquire(self.id, "nova", "ipv4")
        await self.service.acquire(self.id, "nova", "ipv4")
        with self.assertRaises(web.HTTPConflict):
            await self.service.acquire(second, "nova", "ipv4")
        self.service.last_started["nova"] -= 10
        await self.service.acquire(second, "nova", "ipv4")
        self.assertEqual(len(self.service.jobs), 2)

    async def test_parallel_tunnels_are_independent_and_each_uses_novaproxy(self):
        self.service.configure({"nova": dict(instance(), maxConcurrent=2, intervalSeconds=0)}, 0)
        ids = [self.id, "22222222-2222-4222-8222-222222222222"]
        for job_id in ids:
            await self.service.acquire(job_id, "nova", "ipv4")
        await self.ready()
        async def echo(reader, writer):
            try:
                while chunk := await reader.read(1024):
                    writer.write(chunk)
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
        proxy = await asyncio.start_server(self.service.connect, "127.0.0.1", 0)
        calls = []
        async def connect(credentials, target):
            calls.append((credentials["username"], target))
            reader, writer = await asyncio.open_connection("127.0.0.1", upstream.sockets[0].getsockname()[1])
            return reader, writer, b""
        streams = []
        try:
            with patch.object(self.service.novaproxy, "connect", connect):
                for job_id in ids:
                    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
                    streams.append((reader, writer))
                    secret = self.journal.job(job_id)["secret"]
                    auth = base64.b64encode(f"{job_id}:{secret}".encode())
                    writer.write(b"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com:443\r\nProxy-Authorization: Basic " + auth + b"\r\n\r\n")
                    await writer.drain()
                    self.assertIn(b"200", await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2))
                self.assertEqual(calls, [("test-country-us", b"api.openai.com:443")] * 2)
                await self.service.release(ids[0])
                await self.service.jobs[ids[0]]["task"]
                reader, writer = streams[1]
                writer.write(b"still-connected")
                await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(15), 2), b"still-connected")
                self.assertEqual(self.journal.job(ids[1])["state"], "connected")
        finally:
            for _, writer in streams:
                writer.close()
                await writer.wait_closed()
            await self.service.stop()
            proxy.close()
            upstream.close()
            await proxy.wait_closed()
            await upstream.wait_closed()

    def test_limits_reject_invalid_numbers_and_parallel_azure(self):
        for change in ({"maxConcurrent": 0}, {"maxConcurrent": 17}, {"maxConcurrent": True},
                       {"intervalSeconds": -1}, {"intervalSeconds": 3601}, {"intervalSeconds": 0.5}):
            with self.assertRaises(web.HTTPBadRequest):
                validate_instances({"nova": dict(instance(), **change)})
        azure = test_egress.configuration()["instances"]
        azure["azure"]["maxConcurrent"] = 2
        with self.assertRaises(web.HTTPBadRequest):
            validate_instances(azure)

    async def test_save_redacts_password_and_retains_it_on_edit_and_restart(self):
        self.service.configure({"nova": instance()}, 0)
        public = self.service.snapshot()["instances"][0]
        self.assertNotIn("password", public)
        self.assertTrue(public["passwordSet"])
        edited = {k: v for k, v in public.items() if k not in ("id", "families", "passwordSet")}
        edited["name"] = "Renamed"
        self.service.configure({"nova": edited}, 1)
        restarted = Service(test_egress.configuration(), self.journal, self.provider)
        self.assertEqual(restarted.config["instances"]["nova"]["password"], "private-password")
        self.assertNotIn("private-password", json.dumps(restarted.snapshot()))
        edited["password"] = "replacement"
        self.service.configure({"nova": edited}, 2)
        self.assertEqual(self.service.config["instances"]["nova"]["password"], "replacement")
        with self.assertRaises(web.HTTPConflict):
            self.service.configure({"nova": dict(edited, password="stale")}, 2)
        self.assertEqual(self.service.config["instances"]["nova"]["password"], "replacement")

    async def test_new_instance_requires_password_and_cannot_reuse_other_instance_secret(self):
        self.service.configure({"nova": instance()}, 0)
        incomplete = instance()
        incomplete.pop("password")
        with self.assertRaises(web.HTTPBadRequest):
            self.service.configure({"other": incomplete}, 1)

    async def test_legacy_credentials_migrate_once_without_file_dependency(self):
        legacy = {"provider": "novaproxy", "name": "Legacy", "credentialRef": "legacy", "bindings": {"ipv4": {}}}
        self.journal.execute("INSERT OR REPLACE INTO settings VALUES(1,?,?)", (json.dumps({"nova": legacy}), 5))
        with patch.object(NovaProxy, "credentials", return_value={"username": "legacy-user", "password": "legacy-secret"}) as read:
            migrated = Service(test_egress.configuration(), self.journal, self.provider)
            self.assertEqual(migrated.revision, 6)
            self.assertEqual(migrated.config["instances"]["nova"]["password"], "legacy-secret")
            restarted = Service(test_egress.configuration(), self.journal, self.provider)
            self.assertEqual(read.call_count, 1)
            self.assertNotIn("legacy-secret", json.dumps(restarted.snapshot()))
            self.assertNotIn("credentialRef", restarted.config["instances"]["nova"])

    async def test_nova_single_use_tunnel_and_no_azure_allocation(self):
        self.service.configure({"nova": instance()}, 0)
        with patch.object(self.service.novaproxy, "credentials", return_value={"username": "test", "password": "secret"}):
            await self.service.acquire(self.id, "nova", "ipv4")
            await self.ready()
        connections = []
        async def gateway(reader, writer):
            try:
                greeting = await reader.readexactly(3)
                self.assertEqual(greeting, b"\x05\x01\x02")
                writer.write(b"\x05\x02")
                await writer.drain()
                self.assertEqual(await reader.readexactly(1), b"\x01")
                username_length = (await reader.readexactly(1))[0]
                username = await reader.readexactly(username_length)
                password_length = (await reader.readexactly(1))[0]
                password = await reader.readexactly(password_length)
                writer.write(b"\x01\x00")
                await writer.drain()
                request = await reader.readexactly(4)
                self.assertEqual(request, b"\x05\x01\x00\x03")
                host_length = (await reader.readexactly(1))[0]
                connections.append(
                    username + b":" + password + await reader.readexactly(host_length) + await reader.readexactly(2)
                )
                writer.write(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x00")
                await writer.drain()
                writer.write(await reader.readexactly(4))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        upstream = await asyncio.start_server(gateway, "127.0.0.1", 0)
        proxy = await asyncio.start_server(self.service.connect, "127.0.0.1", 0)
        original = asyncio.open_connection
        async def dial(host, port):
            self.assertEqual(host, "residential-gateway.novaproxy.io")
            return await original("127.0.0.1", upstream.sockets[0].getsockname()[1])
        secret = self.journal.job(self.id)["secret"]
        auth = base64.b64encode(f"{self.id}:{secret}".encode()).decode()
        request = f"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com\r\nProxy-Authorization: Basic {auth}\r\n\r\n".encode()
        try:
            with patch("novaproxy.asyncio.open_connection", dial):
                reader, writer = await original("127.0.0.1", proxy.sockets[0].getsockname()[1])
                writer.write(request)
                await writer.drain()
                self.assertIn(b"200", await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5))
                writer.write(b"ping")
                await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(4), 5), b"ping")
                writer.close()
                await writer.wait_closed()
                await self.service.stop()
                reader, writer = await original("127.0.0.1", proxy.sockets[0].getsockname()[1])
                writer.write(request)
                await writer.drain()
                self.assertIn(b"403", await asyncio.wait_for(reader.read(), 5))
                writer.close()
                await writer.wait_closed()
            self.assertEqual(len(connections), 1)
            self.assertEqual(self.provider.allocations, 0)
            self.assertNotIn(auth.encode(), b"".join(connections))
        finally:
            proxy.close()
            upstream.close()
            await proxy.wait_closed()
            await upstream.wait_closed()

    async def test_nova_lease_has_no_claimed_ip_or_exposed_credentials(self):
        self.service.configure({"nova": instance()}, 0)
        with patch.object(self.service.novaproxy, "credentials", return_value={"username": "test", "password": "private-password"}):
            await self.service.acquire(self.id, "nova", "ipv4")
            await self.ready()
        lease = self.service.present(self.journal.job(self.id))
        self.assertIsNone(lease["ip"])
        self.assertEqual(lease["ipVerification"], "unverified")
        self.assertEqual(self.provider.allocations, 0)
        self.assertNotIn("private-password", json.dumps(self.service.snapshot()))
        await self.service.release(self.id)
        await self.service.jobs[self.id]["task"]
        self.assertFalse(self.service.jobs)
