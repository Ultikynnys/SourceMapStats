import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import certs


VALID_CERT = {
    "notBefore": "Apr 26 00:00:00 2026 GMT",
    "notAfter": "Apr 26 00:00:00 2027 GMT",
    "subjectAltName": (("DNS", "example.test"),),
}


class TestCertificateValidation(unittest.TestCase):
    def test_missing_certificate_raises(self):
        with patch("certs.os.path.isfile", side_effect=lambda path: path == "key.pem"):
            with self.assertRaisesRegex(
                certs.CertificateValidationError,
                "certificate file does not exist",
            ):
                certs.validate_tls_certificate(
                    "example.test",
                    "missing.pem",
                    "key.pem",
                )

    @patch("certs.os.path.isfile", return_value=True)
    @patch("certs.ssl.SSLContext")
    @patch("certs.ssl._ssl._test_decode_cert", return_value=VALID_CERT)
    def test_valid_certificate_loads_key_pair(
        self,
        _decode_cert,
        context_cls,
        _isfile,
    ):
        context = Mock()
        context_cls.return_value = context

        certs.validate_tls_certificate(
            "example.test",
            "cert.pem",
            "key.pem",
            now=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )

        context.load_cert_chain.assert_called_once_with(
            certfile="cert.pem",
            keyfile="key.pem",
        )

    @patch("certs.os.path.isfile", return_value=True)
    @patch("certs.ssl._ssl._test_decode_cert")
    def test_expired_certificate_raises(self, decode_cert, _isfile):
        decode_cert.return_value = {
            **VALID_CERT,
            "notAfter": "Apr 26 00:00:00 2025 GMT",
        }

        with self.assertRaisesRegex(
            certs.CertificateValidationError,
            "has expired",
        ):
            certs.validate_tls_certificate(
                "example.test",
                "cert.pem",
                "key.pem",
                now=datetime(2026, 5, 1, tzinfo=timezone.utc),
            )

    @patch("certs.os.path.isfile", return_value=True)
    @patch("certs.ssl._ssl._test_decode_cert", return_value=VALID_CERT)
    def test_hostname_mismatch_raises(self, _decode_cert, _isfile):
        with self.assertRaisesRegex(
            certs.CertificateValidationError,
            "not valid for domain",
        ):
            certs.validate_tls_certificate(
                "wrong.example.test",
                "cert.pem",
                "key.pem",
                now=datetime(2026, 5, 1, tzinfo=timezone.utc),
            )

    @patch("certs.socket.create_connection")
    @patch("certs.ssl.create_default_context")
    def test_live_tls_endpoint_uses_verified_hostname(self, context_factory, connect):
        context = Mock()
        context_factory.return_value = context
        socket_context = Mock()
        tls_context = Mock()
        connect.return_value.__enter__.return_value = socket_context
        context.wrap_socket.return_value = MagicMock()
        context.wrap_socket.return_value.__enter__.return_value = tls_context

        certs.validate_live_tls_endpoint("example.test", port=443, timeout=2)

        connect.assert_called_once_with(("example.test", 443), timeout=2)
        context.wrap_socket.assert_called_once_with(
            socket_context,
            server_hostname="example.test",
        )

    @patch("certs.socket.create_connection", side_effect=OSError("refused"))
    def test_live_tls_endpoint_failure_raises(self, _connect):
        with self.assertRaisesRegex(
            certs.CertificateValidationError,
            "Live TLS validation failed",
        ):
            certs.validate_live_tls_endpoint("example.test", port=443, timeout=2)


if __name__ == "__main__":
    unittest.main()
