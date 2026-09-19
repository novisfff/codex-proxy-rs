import asyncio
import base64
import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from egress import Azure, EgressError, Journal, Service, application, validate_instances


def configuration():
    return {"proxyUrl": "http://127.0.0.1:19081", "instances": {
        "azure": {"provider": "azure", "name": "Test", "subscription": "test-subscription",
                  "resourceGroup": "test-resources", "location": "eastasia", "bindings": {
                      "ipv4": {"resourceGroup": "test-resources", "nic": "dedicated",
                               "ipConfiguration": "secondary", "sourceIp": "10.0.0.9", "dedicated": True}}}}}


class FakeProvider:
    def __init__(self):
        self.allocations = 0
        self.cleanups = 0
        self.provision = asyncio.Event()
        self.provision.set()
        self.fail_cleanup = False

    async def start(self):
        pass

    async def allocate(self, instance, family):
        self.allocations += 1
        await self.provision.wait()
        return "203.0.113.9", "127.0.0.1"

    async def recover(self):
        self.cleanups += 1
        if self.fail_cleanup:
            raise EgressError()


class JournalTests(unittest.TestCase):
    def test_restart_and_canonical_ipv6_dedup(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.db")
            first = Journal(path)
            self.assertTrue(first.reserve("2001:db8:0::1", 100000))
            first.db.close()
            second = Journal(path)
            self.assertFalse(second.reserve("2001:db8::1", 186399))
            self.assertTrue(second.reserve("2001:db8::1", 186400))
            second.db.close()

    def test_reject_wrong_family_and_duplicate_binding(self):
        config = configuration()["instances"]
        validate_instances(config)
        config["azure"]["bindings"]["ipv6"] = copy.deepcopy(config["azure"]["bindings"]["ipv4"])
        with self.assertRaises(web.HTTPBadRequest):
            validate_instances(config)


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.journal = Journal(str(Path(self.directory.name) / "state.db"))
        self.provider = FakeProvider()
        self.service = Service(configuration(), self.journal, self.provider)
        await self.service.start()
        self.id = "11111111-1111-4111-8111-111111111111"

    async def asyncTearDown(self):
        self.provider.provision.set()
        await self.service.stop()
        self.journal.db.close()
        self.directory.cleanup()

    async def ready(self):
        for _ in range(100):
            await asyncio.sleep(0)
            if self.journal.job(self.id)["state"] == "ready":
                return
        self.fail("lease was not ready")

    async def test_idempotency_exclusivity_and_release(self):
        self.provider.provision.clear()
        await self.service.acquire(self.id, "azure", "ipv4")
        await self.service.acquire(self.id, "azure", "ipv4")
        await asyncio.sleep(0)
        self.assertEqual(self.provider.allocations, 1)
        with self.assertRaises(web.HTTPConflict):
            await self.service.acquire("22222222-2222-4222-8222-222222222222", "azure", "ipv4")
        await self.service.release(self.id)
        self.provider.provision.set()
        await self.service.jobs[self.id]["task"]
        self.assertEqual(self.journal.job(self.id)["state"], "released")
        self.assertFalse(self.service.jobs)
        self.assertEqual(self.provider.cleanups, 1)

    async def test_cleanup_failure_blocks_new_allocation(self):
        await self.service.acquire(self.id, "azure", "ipv4")
        await self.ready()
        self.provider.fail_cleanup = True
        await self.service.release(self.id)
        await self.service.jobs[self.id]["task"]
        self.assertFalse(self.service.snapshot()["available"])
        with self.assertRaises(web.HTTPServiceUnavailable):
            await self.service.acquire("22222222-2222-4222-8222-222222222222", "azure", "ipv4")

    async def test_control_auth_and_no_secrets_in_status(self):
        client = TestClient(TestServer(application(self.service, "test-control-secret")))
        await client.start_server()
        try:
            response = await client.get("/v1/status")
            self.assertEqual(response.status, 401)
            await self.service.acquire(self.id, "azure", "ipv4")
            await self.ready()
            response = await client.get("/v1/status", headers={"Authorization": "Bearer test-control-secret"})
            body = await response.text()
            self.assertEqual(response.status, 200)
            self.assertNotIn(self.journal.job(self.id)["secret"], body)
        finally:
            await client.close()

    async def test_configuration_persists_and_cannot_change_while_busy(self):
        config = configuration()["instances"]
        config["azure"]["name"] = "Renamed"
        self.service.configure(config, 0)
        with self.assertRaises(web.HTTPConflict):
            self.service.configure({}, 0)
        second = Service(configuration(), self.journal, self.provider)
        self.assertEqual(second.snapshot()["instances"][0]["name"], "Renamed")
        await self.service.acquire(self.id, "azure", "ipv4")
        with self.assertRaises(web.HTTPConflict):
            self.service.configure({}, 1)

    async def test_connect_tunnels_bytes_once_and_rejects_other_destinations(self):
        async def echo(reader, writer):
            writer.write(await reader.read(4))
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        echo_server = await asyncio.start_server(echo, "127.0.0.1", 0)
        proxy = await asyncio.start_server(self.service.connect, "127.0.0.1", 0)
        proxy_port = proxy.sockets[0].getsockname()[1]
        await self.service.acquire(self.id, "azure", "ipv4")
        await self.ready()
        secret = self.journal.job(self.id)["secret"]
        auth = base64.b64encode(f"{self.id}:{secret}".encode()).decode()
        original_connect = asyncio.open_connection

        async def connect_local(host, port, **kwargs):
            self.assertEqual(kwargs["local_addr"], ("127.0.0.1", 0))
            return await original_connect("127.0.0.1", echo_server.sockets[0].getsockname()[1])

        try:
            reader, writer = await original_connect("127.0.0.1", proxy_port)
            writer.write(f"CONNECT evil.example:443 HTTP/1.1\r\nHost: evil.example\r\nProxy-Authorization: Basic {auth}\r\n\r\n".encode())
            await writer.drain()
            self.assertIn(b"403", await reader.read())
            writer.close()
            await writer.wait_closed()
            self.assertEqual(self.journal.job(self.id)["state"], "ready")
            reader, writer = await original_connect("127.0.0.1", proxy_port)
            with patch("egress.asyncio.open_connection", connect_local):
                writer.write(f"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com\r\nProxy-Authorization: Basic {auth}\r\n\r\n".encode())
                await writer.drain()
                self.assertIn(b"200", await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10))
                writer.write(b"ping")
                await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(4), 5), b"ping")
                writer.close()
                await writer.wait_closed()
            await self.service.stop()
            reader, writer = await original_connect("127.0.0.1", proxy_port)
            writer.write(f"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com\r\nProxy-Authorization: Basic {auth}\r\n\r\n".encode())
            await writer.drain()
            self.assertIn(b"403", await reader.read())
            writer.close()
            await writer.wait_closed()
        finally:
            proxy.close()
            echo_server.close()
            await proxy.wait_closed()
            await echo_server.wait_closed()


class AzureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.journal = Journal(str(Path(self.directory.name) / "state.db"))
        self.azure = Azure(configuration()["instances"], self.journal, "test-owner")

    async def asyncTearDown(self):
        self.journal.db.close()
        self.directory.cleanup()

    async def test_repeated_ip_is_deleted_before_binding(self):
        self.journal.reserve("203.0.113.1", time.time())
        config, binding = self.azure.binding("azure", "ipv4")
        commands, deletions = [], []
        async def preflight(*_):
            return config, binding
        async def command(*args):
            commands.append(args)
            if args[:3] == ("network", "public-ip", "create"):
                number = sum(c[:3] == ("network", "public-ip", "create") for c in commands)
                return {"publicIp": {"id": f"public-{number}", "ipAddress": f"203.0.113.{number}"}}
        async def cleanup(name, *_):
            deletions.append(name)
            self.journal.execute("DELETE FROM resources WHERE name=?", (name,))
        async def verify(source, family, public):
            self.assertEqual((source, family, public), ("10.0.0.9", "ipv4", "203.0.113.2"))
        with patch.object(self.azure, "preflight", preflight), patch.object(self.azure, "command", command), patch.object(self.azure, "cleanup", cleanup), patch.object(self.azure, "verify", verify), patch.object(self.azure, "cleanup_unused_public_ips", AsyncMock()):
            address, _ = await self.azure.allocate("azure", "ipv4")
        self.assertEqual(address, "203.0.113.2")
        self.assertEqual(len(deletions), 1)
        associations = [c for c in commands if c[:4] == ("network", "nic", "ip-config", "update")]
        self.assertEqual(len(associations), 1)
        self.assertEqual(associations[0][-1], "public-2")

    async def test_primary_ipv4_is_never_modified(self):
        async def nic(*_):
            return {"provisioningState": "Succeeded", "ipConfigurations": [{"name": "secondary", "primary": True, "privateIPAddress": "10.0.0.9"}]}
        with patch.object(self.azure, "nic", nic):
            with self.assertRaises(EgressError):
                await self.azure.preflight("azure", "ipv4")

    async def test_ipv6_source_requires_deprecated_preference(self):
        for lifetime in (3600, 0):
            output = json.dumps([{"addr_info": [{"local": "2001:db8::9", "preferred_life_time": lifetime}]}]).encode()
            process = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(output, None)))
            with patch("egress.asyncio.create_subprocess_exec", AsyncMock(return_value=process)):
                if lifetime:
                    with self.assertRaises(EgressError):
                        await self.azure.check_ipv6_source("2001:db8::9")
                else:
                    await self.azure.check_ipv6_source("2001:db8::9")

    async def test_ipv6_allocation_binds_ipv6_private_source(self):
        config = self.azure.instances["azure"]
        binding = dict(config["bindings"]["ipv4"], sourceIp="2001:db8::9")
        config["bindings"]["ipv6"] = binding
        commands = []
        async def preflight(*_):
            return config, binding
        async def command(*args):
            commands.append(args)
            if args[:3] == ("network", "public-ip", "create"):
                self.assertEqual(args[args.index("--version") + 1], "IPv6")
                return {"publicIp": {"id": "public-v6", "ipAddress": "2001:db8::42"}}
        async def verify(source, family, address):
            self.assertEqual((source, family, address), ("2001:db8::9", "ipv6", "2001:db8::42"))
        with patch.object(self.azure, "preflight", preflight), patch.object(self.azure, "command", command), patch.object(self.azure, "verify", verify), patch.object(self.azure, "cleanup_unused_public_ips", AsyncMock()):
            self.assertEqual(await self.azure.allocate("azure", "ipv6"), ("2001:db8::42", "2001:db8::9"))
        self.assertTrue(any(c[:4] == ("network", "nic", "ip-config", "update") for c in commands))

    async def test_uncertain_creation_does_not_discard_recovery_record(self):
        self.journal.execute("INSERT INTO resources(name,instance,family) VALUES('pending','azure','ipv4')")
        async def command(*_):
            return []
        with patch.object(self.azure, "command", command):
            with self.assertRaises(EgressError):
                await self.azure.recover()
        self.assertEqual(self.journal.db.execute("SELECT count(*) FROM resources").fetchone()[0], 1)

    async def test_stale_cleanup_protects_primary_and_deletes_unowned_ipv4_ipv6(self):
        primary = {"name": "primary", "id": "/primary", "ipConfiguration": {"id": "/nic/primary"}}
        stale = [{"name": "old4", "id": "/old4", "ipAddress": "203.0.113.7", "provisioningState": "Succeeded"},
                 {"name": "old6", "id": "/old6", "ipAddress": "2001:db8::7", "provisioningState": "Succeeded"}]
        nic = {"ipConfigurations": [{"primary": True, "publicIPAddress": {"id": "/PRIMARY"}}]}
        command = AsyncMock(side_effect=[[primary, *stale], None, None])
        with patch.object(self.azure, "nic", AsyncMock(return_value=nic)), patch.object(self.azure, "command", command):
            await self.azure.cleanup_unused_public_ips(self.azure.instances["azure"])
        self.assertEqual([call.args[-1] for call in command.call_args_list[1:]], ["old4", "old6"])
        self.assertFalse(self.journal.reserve("203.0.113.7", time.time()))
        self.assertFalse(self.journal.reserve("2001:db8::7", time.time()))

    async def test_stale_cleanup_fails_closed_for_attached_or_unready_ip(self):
        nic = {"ipConfigurations": [{"primary": True, "publicIPAddress": {"id": "/primary"}}]}
        for extra in ({"ipConfiguration": {"id": "/other"}}, {"natGateway": {"id": "/nat"}},
                      {"provisioningState": "Updating"}):
            public = dict({"name": "other", "id": "/other", "provisioningState": "Succeeded"}, **extra)
            command = AsyncMock(return_value=[public])
            with patch.object(self.azure, "nic", AsyncMock(return_value=nic)), patch.object(self.azure, "command", command):
                with self.assertRaises(EgressError):
                    await self.azure.cleanup_unused_public_ips(self.azure.instances["azure"])
            self.assertEqual(command.await_count, 1)

    async def test_stale_cleanup_requires_primary_identification(self):
        command = AsyncMock()
        with patch.object(self.azure, "nic", AsyncMock(return_value={"ipConfigurations": []})), patch.object(self.azure, "command", command):
            with self.assertRaises(EgressError):
                await self.azure.cleanup_unused_public_ips(self.azure.instances["azure"])
        command.assert_not_awaited()

    async def test_cleanup_refuses_foreign_resources(self):
        self.journal.execute("INSERT INTO resources(name,instance,family) VALUES('foreign','azure','ipv4')")
        async def command(*args):
            self.assertEqual(args[:3], ("network", "public-ip", "list"))
            return [{"name": "foreign", "tags": {"cpr292-owner": "someone-else"}}]
        with patch.object(self.azure, "command", command):
            with self.assertRaises(EgressError):
                await self.azure.recover()


if __name__ == "__main__":
    unittest.main()
