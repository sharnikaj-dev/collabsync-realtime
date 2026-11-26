"""Operational Transform and deterministic tie-breaking tests."""

import random
import unittest

from server.rooms.ot import (
    SpliceOp,
    apply_ops,
    transform_against_history,
    transform_cursor,
)


class TestOperationalTransform(unittest.TestCase):
    def test_disjoint_concurrent_inserts(self) -> None:
        doc = "hello world"
        op_a = SpliceOp(position=0, delete_len=0, insert_text=">> ", user_id="u1", timestamp=1.0)
        op_b = SpliceOp(position=11, delete_len=0, insert_text="!", user_id="u2", timestamp=2.0)

        # Suppose op_a commits first, then op_b arrives with base_version=0
        doc_after_a = apply_ops(doc, [op_a])
        op_b_prime = transform_against_history(op_b, [op_a])
        doc_final_1 = apply_ops(doc_after_a, op_b_prime)

        # Suppose op_b commits first, then op_a arrives with base_version=0
        doc_after_b = apply_ops(doc, [op_b])
        op_a_prime = transform_against_history(op_a, [op_b])
        doc_final_2 = apply_ops(doc_after_b, op_a_prime)

        self.assertEqual(doc_final_1, ">> hello world!")
        self.assertEqual(doc_final_1, doc_final_2)

    def test_same_position_insert_tie_breaking_is_deterministic(self) -> None:
        doc = "base"
        op_a = SpliceOp(position=4, delete_len=0, insert_text="A", user_id="user_a", timestamp=10.0)
        op_b = SpliceOp(position=4, delete_len=0, insert_text="B", user_id="user_b", timestamp=10.0)

        # Order 1: A then B
        doc1 = apply_ops(doc, [op_a])
        doc1 = apply_ops(doc1, transform_against_history(op_b, [op_a]))

        # Order 2: B then A
        doc2 = apply_ops(doc, [op_b])
        doc2 = apply_ops(doc2, transform_against_history(op_a, [op_b]))

        self.assertEqual(doc1, "baseAB")
        self.assertEqual(doc1, doc2)

    def test_overlapping_deletes_do_not_over_delete(self) -> None:
        doc = "ABCDEF"
        # u1 deletes "BCD" [1, 4)
        op_u1 = SpliceOp(position=1, delete_len=3, insert_text="", user_id="u1", timestamp=1.0)
        # u2 concurrently deletes "CDE" [2, 5)
        op_u2 = SpliceOp(position=2, delete_len=3, insert_text="", user_id="u2", timestamp=2.0)

        doc1 = apply_ops(doc, [op_u1])
        op_u2_prime = transform_against_history(op_u2, [op_u1])
        doc1 = apply_ops(doc1, op_u2_prime)

        doc2 = apply_ops(doc, [op_u2])
        op_u1_prime = transform_against_history(op_u1, [op_u2])
        doc2 = apply_ops(doc2, op_u1_prime)

        self.assertEqual(doc1, "AF")
        self.assertEqual(doc1, doc2)

    def test_enclosing_replace_preserves_inner_insertion(self) -> None:
        doc = "ABCDEF"
        # u1 replaces "CD" with "XY" -> "ABXYEF"
        op_u1 = SpliceOp(position=2, delete_len=2, insert_text="XY", user_id="u1", timestamp=1.0)
        # u2 concurrently replaces "BCDE" [1, 5) with "Z"
        op_u2 = SpliceOp(position=1, delete_len=4, insert_text="Z", user_id="u2", timestamp=2.0)

        doc_after_u1 = apply_ops(doc, [op_u1])
        transformed = transform_against_history(op_u2, [op_u1])
        doc_final = apply_ops(doc_after_u1, transformed)
        self.assertEqual(doc_final, "AZXYF")

    def test_multi_revision_chain_convergence_with_split_op(self) -> None:
        doc = "ABCDEF"
        c1 = SpliceOp(position=2, delete_len=2, insert_text="XY", user_id="u1", timestamp=1.0)
        doc_v1 = apply_ops(doc, [c1])  # ABXYEF
        c2 = SpliceOp(position=3, delete_len=0, insert_text="!", user_id="u3", timestamp=1.5)
        doc_v2 = apply_ops(doc_v1, [c2])  # ABX!YEF

        op_late = SpliceOp(position=1, delete_len=4, insert_text="ZZZ", user_id="u2", timestamp=2.0)
        transformed = transform_against_history(op_late, [c1, c2])
        doc_v3 = apply_ops(doc_v2, transformed)
        self.assertEqual(doc_v3, "AZZZX!YF")

    def test_cursor_transform_shifts_with_concurrent_edits(self) -> None:
        op1 = SpliceOp(position=0, delete_len=0, insert_text="abc", user_id="u1", timestamp=1.0)
        op2 = SpliceOp(position=5, delete_len=2, insert_text="", user_id="u1", timestamp=2.0)
        self.assertEqual(transform_cursor(10, [op1, op2]), 11)
        self.assertEqual(transform_cursor(0, [op1]), 0)

    def test_randomized_diamond_convergence(self) -> None:
        """Fuzz-test 200 pairs of random concurrent pure inserts/deletes for diamond convergence."""
        rng = random.Random(42)
        for _ in range(200):
            base = "".join(rng.choices("abcdefghij", k=rng.randint(0, 20)))
            n = len(base)

            def rand_op(uid: str, ts: float) -> SpliceOp:
                if n == 0 or rng.random() < 0.6:
                    pos = rng.randint(0, n)
                    txt = "".join(rng.choices("XYZ123", k=rng.randint(1, 4)))
                    return SpliceOp(position=pos, delete_len=0, insert_text=txt, user_id=uid, timestamp=ts)
                pos = rng.randint(0, n - 1)
                dlen = rng.randint(1, n - pos)
                return SpliceOp(position=pos, delete_len=dlen, insert_text="", user_id=uid, timestamp=ts)

            op_a = rand_op("a", 1.0)
            op_b = rand_op("b", 2.0)

            res1 = apply_ops(apply_ops(base, [op_a]), transform_against_history(op_b, [op_a]))
            res2 = apply_ops(apply_ops(base, [op_b]), transform_against_history(op_a, [op_b]))
            self.assertEqual(res1, res2)


if __name__ == "__main__":
    unittest.main()
