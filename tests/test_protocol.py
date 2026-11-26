"""Protocol parser and validation tests."""

import json
import unittest

from server.protocol import MessageType, ProtocolError, parse_client_message


class TestProtocol(unittest.TestCase):
    def test_valid_join_room(self) -> None:
        msg = parse_client_message(
            json.dumps(
                {
                    "type": "JOIN_ROOM",
                    "room_id": "  alpha-pad  ",
                    "display_name": "Ada Lovelace",
                }
            )
        )
        self.assertEqual(msg["type"], MessageType.JOIN_ROOM.value)
        self.assertEqual(msg["room_id"], "alpha-pad")
        self.assertEqual(msg["display_name"], "Ada Lovelace")

    def test_valid_state_patch_insert(self) -> None:
        msg = parse_client_message(
            {
                "type": "STATE_PATCH",
                "room_id": "room-1",
                "base_version": 3,
                "client_seq": 7,
                "op": {"kind": "insert", "position": 12, "text": "hello"},
            }
        )
        self.assertEqual(msg["base_version"], 3)
        self.assertEqual(msg["client_seq"], 7)
        self.assertEqual(msg["op"]["kind"], "insert")
        self.assertEqual(msg["op"]["position"], 12)
        self.assertEqual(msg["op"]["text"], "hello")
        self.assertEqual(msg["op"]["length"], 0)

    def test_invalid_json_raises_protocol_error(self) -> None:
        with self.assertRaises(ProtocolError) as ctx:
            parse_client_message("{broken json")
        self.assertEqual(ctx.exception.code, "INVALID_JSON")

    def test_unknown_message_type_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as ctx:
            parse_client_message({"type": "HACK_SERVER", "room_id": "r1"})
        self.assertEqual(ctx.exception.code, "UNKNOWN_TYPE")

    def test_negative_version_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as ctx:
            parse_client_message(
                {
                    "type": "STATE_PATCH",
                    "room_id": "r1",
                    "base_version": -1,
                    "op": {"kind": "insert", "position": 0, "text": "x"},
                }
            )
        self.assertEqual(ctx.exception.code, "INVALID_FIELD")

    def test_insert_with_nonzero_length_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as ctx:
            parse_client_message(
                {
                    "type": "STATE_PATCH",
                    "room_id": "r1",
                    "base_version": 0,
                    "op": {"kind": "insert", "position": 0, "text": "a", "length": 2},
                }
            )
        self.assertEqual(ctx.exception.code, "INVALID_OP")

    def test_delete_with_text_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as ctx:
            parse_client_message(
                {
                    "type": "STATE_PATCH",
                    "room_id": "r1",
                    "base_version": 0,
                    "op": {"kind": "delete", "position": 0, "text": "abc", "length": 3},
                }
            )
        self.assertEqual(ctx.exception.code, "INVALID_OP")


if __name__ == "__main__":
    unittest.main()
