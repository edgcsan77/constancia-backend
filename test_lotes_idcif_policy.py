import unittest

from lotes_idcif_policy import (
    classify_not_eligible_error,
    reason_label,
)


class PolicyTests(unittest.TestCase):
    def test_baja(self):
        self.assertEqual(
            classify_not_eligible_error("CLIENT_RFC_NOT_ACTIVE:BAJA"),
            "RFC_DADO_DE_BAJA",
        )

    def test_cancelado(self):
        self.assertEqual(
            classify_not_eligible_error("CLIENT_RFC_NOT_ACTIVE:CANCELADO"),
            "RFC_CANCELADO",
        )

    def test_inactivo(self):
        self.assertEqual(
            classify_not_eligible_error("CLIENT_RFC_NOT_ACTIVE:INACTIVO"),
            "RFC_INACTIVO",
        )

    def test_sin_estatus(self):
        self.assertEqual(
            classify_not_eligible_error("CLIENT_RFC_STATUS_MISSING"),
            "RFC_SIN_ESTATUS",
        )

    def test_suspendido_not_mislabeled(self):
        self.assertEqual(
            classify_not_eligible_error("CLIENT_RFC_NOT_ACTIVE:SUSPENDIDO"),
            "SAT_TEMPORAL_ERROR",
        )

    def test_label(self):
        self.assertEqual(reason_label("IDCIF_INVALID"), "ID INCORRECTO")


if __name__ == "__main__":
    unittest.main()
