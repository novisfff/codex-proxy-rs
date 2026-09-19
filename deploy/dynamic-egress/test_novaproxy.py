import asyncio
import base64
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp import web
from egress import validate_instances
from novaproxy import NovaProxy
import test_egress


def instance():
    return {"provider": "novaproxy", "name": "Nova test", "credentialRef": "test-us", "bindings": {"ipv4": {}}}


class NovaTests(unittest.IsolatedAsyncioTestCase):
    def test_configuration_rejects_ipv6_credentials_and_paths(self):
        validate_instances({"nova": instance()})
        for change in ({"bindings": {"ipv6": {}}}, {"password": "secret"}, {"credentialRef": "../secret"}):
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
        response = b"HTTP/1.1 200 Connection established\r\n\r\nhello"
        async def gateway(reader, writer):
            requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(gateway, "127.0.0.1", 0)
        original = asyncio.open_connection
        async def dial(host, port):
            self.assertEqual((host, port), ("residential-gateway.novaproxy.io", 1111))
            return await original("127.0.0.1", server.sockets[0].getsockname()[1])
        try:
            with patch("novaproxy.asyncio.open_connection", dial):
                reader, writer, trailing = await NovaProxy().connect({"username": "test", "password": "secret"}, b"api.openai.com:443")
                self.assertEqual(trailing + await reader.read(), b"hello")
                writer.close()
                await writer.wait_closed()
                self.assertIn(b"CONNECT api.openai.com:443 HTTP/1.1", requests[0])
                self.assertIn(base64.b64encode(b"test:secret"), requests[0])
                response = b"HTTP/1.1 407 Auth required\r\nContent-Length: 0\r\n\r\n"
                with self.assertRaisesRegex(ConnectionError, "^NovaProxy CONNECT rejected$"):
                    await NovaProxy().connect({"username": "test", "password": "secret"}, b"api.openai.com:443")
                self.assertEqual(len(requests), 2)
        finally:
            server.close()
            await server.wait_closed()


class NovaServiceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_egress.ServiceTests.asyncSetUp
    asyncTearDown = test_egress.ServiceTests.asyncTearDown
    ready = test_egress.ServiceTests.ready

    async def test_nova_single_use_tunnel_and_no_azure_allocation(self):
        self.service.configure({"nova": instance()}, 0)
        with patch.object(self.service.novaproxy, "credentials", return_value={"username": "test", "password": "secret"}):
            await self.service.acquire(self.id, "nova", "ipv4")
            await self.ready()
        connections = []
        async def gateway(reader, writer):
            connections.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            writer.write(await reader.readexactly(4))
            await writer.drain()
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
                await self.service.task
                reader, writer = await original("127.0.0.1", proxy.sockets[0].getsockname()[1])
                writer.write(request)
                await writer.drain()
                self.assertIn(b"403", await asyncio.wait_for(reader.read(), 5))
                writer.close()
                await writer.wait_closed()
            self.assertEqual(len(connections), 1)
            self.assertEqual(self.provider.allocations, 0)
            self.assertNotIn(auth.encode(), connections[0])
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
        await self.service.task
        self.assertIsNone(self.service.nova_credentials)
