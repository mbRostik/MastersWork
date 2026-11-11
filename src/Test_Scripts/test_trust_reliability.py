import json
import unittest
from itertools import combinations
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from clickhouse_driver.errors import ErrorCodes, ServerException, SocketTimeoutError

from AnalyticService import ClickHouseUnavailable, process_analytics_message
from ConsensusService import UnexpectedReputationResponse, process_zkp_message
from validator_quorum import AUTH_HEADER, CHALLENGE_HEADER, VOTE_HEADER, Validator, authenticate, collect_votes


class AnalyticsDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_keeps_deduplication_token(self):
        message = SimpleNamespace(
            body=b'{"action_type":"post","commitment":"123","timestamp":"2026-01-01T00:00:00Z"}',
            message_id="event-1",
        )
        client = Mock()
        client.execute.side_effect = [SocketTimeoutError("read timeout"), ConnectionResetError(), 1]
        with patch("AnalyticService.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await process_analytics_message(message, client)
        self.assertEqual([call.args for call in sleep.await_args_list], [(0.5,), (1,)])
        self.assertEqual(client.execute.call_count, 3)
        self.assertTrue(all(call == client.execute.call_args for call in client.execute.call_args_list))
        self.assertEqual(client.execute.call_args.kwargs["settings"]["insert_deduplication_token"], "event-1")

    async def test_write_errors_escape_the_message_handler(self):
        message = SimpleNamespace(
            body=b'{"action_type":"post","commitment":"123","timestamp":"2026-01-01T00:00:00Z"}',
            message_id=None,
        )
        client = Mock()
        with patch("AnalyticService.asyncio.sleep", new_callable=AsyncMock):
            client.execute.side_effect = SocketTimeoutError("read timeout")
            with self.assertRaises(ClickHouseUnavailable):
                await process_analytics_message(message, client)
            self.assertEqual(client.execute.call_count, 3)
            client.reset_mock()
            client.execute.side_effect = ServerException("missing table", code=ErrorCodes.UNKNOWN_TABLE)
            with self.assertRaises(ServerException):
                await process_analytics_message(message, client)
            client.execute.assert_called_once()


class ConsensusDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_confirmed_reputation_update_publishes_approval(self):
        proof = {"commitment": "123", "public_signals": [], "proof_data": {}}
        request = httpx.Request("POST", "https://localhost/user/updatereputation")
        backend = AsyncMock()
        channel = SimpleNamespace(default_exchange=AsyncMock())
        with (
            patch("ConsensusService.verify_proof", return_value=True),
            patch("ConsensusService.proof_envelope", return_value=proof),
            patch("ConsensusService.collect_votes", return_value=7),
        ):
            for status, error in [(200, UnexpectedReputationResponse), (503, httpx.HTTPStatusError)]:
                backend.post.return_value = httpx.Response(status, request=request)
                with self.assertRaises(error):
                    await process_zkp_message(proof, range(10), backend, None, channel, 15)
            channel.default_exchange.publish.assert_not_awaited()

            backend.post.return_value = httpx.Response(204, request=request)
            await process_zkp_message(proof, range(10), backend, None, channel, 15)
            channel.default_exchange.publish.assert_awaited_once()
            event = json.loads(channel.default_exchange.publish.await_args.args[0].body)
            self.assertEqual(event["status"], "APPROVED")


class QuorumTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_placements_of_up_to_three_faulty_validators(self):
        validators = tuple(
            Validator(f"http://127.0.0.1:{8031 + index}/verify", bytes([index]) * 32)
            for index in range(10)
        )
        checked = 0
        for fault_count in range(4):
            for placement in combinations(range(10), fault_count):
                def vote(request):
                    index = request.url.port - 8031
                    correct = json.loads(request.content)["valid"]
                    ballot = "yes" if correct != (index in placement) else "no"
                    signature = authenticate(
                        validators[index].key, request.headers[CHALLENGE_HEADER], request.content, ballot,
                    )
                    return httpx.Response(204, headers={VOTE_HEADER: ballot, AUTH_HEADER: signature})

                async with httpx.AsyncClient(transport=httpx.MockTransport(vote)) as client:
                    self.assertGreaterEqual(await collect_votes(client, validators, {"valid": True}, 2), 7)
                    self.assertEqual(await collect_votes(client, validators, {"valid": False}, 2), fault_count)
                checked += 1
        self.assertEqual(checked, 176)


if __name__ == "__main__":
    unittest.main()
