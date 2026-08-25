import math
import unittest

from nanoq.errors import InvalidPayload, InvalidQueueName
from nanoq.validation import encode_payload, validate_queue_name


class ValidationTests(unittest.TestCase):
    def test_accepts_simple_queue_names(self) -> None:
        self.assertEqual(validate_queue_name("orders.eu-west"), "orders.eu-west")

    def test_rejects_invalid_queue_names(self) -> None:
        for value in ("", " orders", "orders ", "orders\n", 12, None):
            with self.subTest(value=value), self.assertRaises(InvalidQueueName):
                validate_queue_name(value)

    def test_accepts_json_compatible_payload(self) -> None:
        self.assertEqual(encode_payload({"ready": True, "items": [1, None]}), '{"ready":true,"items":[1,null]}')

    def test_rejects_non_json_and_non_finite_values(self) -> None:
        for value in ({1, 2}, object(), math.nan, math.inf):
            with self.subTest(value=value), self.assertRaises(InvalidPayload):
                encode_payload(value)


if __name__ == "__main__":
    unittest.main()

