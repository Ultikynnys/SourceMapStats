import os
import ssl
import ipaddress
import socket
from datetime import datetime, timezone


class CertificateValidationError(RuntimeError):
    """Raised when the configured TLS certificate cannot be trusted."""


def cert_validation_enabled():
    value = os.getenv("REQUIRE_VALID_CERTS", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def get_tls_certificate_config():
    domain = os.getenv("DOMAIN_NAME", "tf2stats.r60d.xyz")
    cert_path = os.getenv(
        "TLS_CERT_FILE",
        f"/etc/letsencrypt/live/{domain}/fullchain.pem",
    )
    key_path = os.getenv(
        "TLS_KEY_FILE",
        f"/etc/letsencrypt/live/{domain}/privkey.pem",
    )
    return domain, cert_path, key_path


def validate_tls_certificate(domain, cert_path, key_path, now=None):
    now = now or datetime.now(timezone.utc)

    for label, path in (("certificate", cert_path), ("private key", key_path)):
        if not path:
            raise CertificateValidationError(f"TLS {label} path is not configured")
        if not os.path.isfile(path):
            raise CertificateValidationError(f"TLS {label} file does not exist: {path}")

    try:
        decoded_cert = ssl._ssl._test_decode_cert(cert_path)
    except Exception as exc:
        raise CertificateValidationError(
            f"TLS certificate could not be parsed: {cert_path}"
        ) from exc

    _validate_certificate_dates(decoded_cert, now, cert_path)
    _validate_certificate_hostname(decoded_cert, domain)
    _validate_cert_key_pair(cert_path, key_path)


def enforce_valid_tls_certificate():
    if not cert_validation_enabled():
        return

    domain, cert_path, key_path = get_tls_certificate_config()
    validate_tls_certificate(domain, cert_path, key_path)
    validate_live_tls_endpoint(domain)


def validate_live_tls_endpoint(domain, port=None, timeout=None):
    if not domain:
        raise CertificateValidationError("DOMAIN_NAME is required for live TLS validation")

    port = int(port or os.getenv("TLS_VERIFY_PORT", "443"))
    timeout = float(timeout or os.getenv("TLS_VERIFY_TIMEOUT_SECONDS", "5"))

    try:
        context = ssl.create_default_context()
        with socket.create_connection((domain, port), timeout=timeout) as sock:
            with context.wrap_socket(sock, server_hostname=domain):
                return
    except Exception as exc:
        raise CertificateValidationError(
            f"Live TLS validation failed for {domain}:{port}"
        ) from exc


def _validate_certificate_dates(decoded_cert, now, cert_path):
    try:
        not_before = ssl.cert_time_to_seconds(decoded_cert["notBefore"])
        not_after = ssl.cert_time_to_seconds(decoded_cert["notAfter"])
    except KeyError as exc:
        raise CertificateValidationError(
            f"TLS certificate is missing validity dates: {cert_path}"
        ) from exc
    except Exception as exc:
        raise CertificateValidationError(
            f"TLS certificate validity dates are invalid: {cert_path}"
        ) from exc

    now_ts = now.timestamp()
    if now_ts < not_before:
        raise CertificateValidationError(
            f"TLS certificate is not valid yet: {cert_path}"
        )
    if now_ts > not_after:
        raise CertificateValidationError(
            f"TLS certificate has expired: {cert_path}"
        )


def _validate_certificate_hostname(decoded_cert, domain):
    if not domain:
        raise CertificateValidationError("DOMAIN_NAME is required for TLS validation")

    san_entries = decoded_cert.get("subjectAltName", ())
    dns_names = [value for kind, value in san_entries if kind == "DNS"]
    ip_names = [value for kind, value in san_entries if kind == "IP Address"]

    if any(_hostname_matches(pattern, domain) for pattern in dns_names):
        return
    if _is_ip_address(domain) and domain in ip_names:
        return

    if not dns_names and not ip_names:
        for subject_part in decoded_cert.get("subject", ()):
            for key, value in subject_part:
                if key == "commonName" and _hostname_matches(value, domain):
                    return

    raise CertificateValidationError(
        f"TLS certificate is not valid for domain: {domain}"
    )


def _hostname_matches(pattern, hostname):
    pattern = pattern.rstrip(".").lower()
    hostname = hostname.rstrip(".").lower()

    if pattern == hostname:
        return True
    if not pattern.startswith("*."):
        return False

    suffix = pattern[1:]
    return hostname.endswith(suffix) and hostname.count(".") == pattern.count(".")


def _is_ip_address(value):
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def _validate_cert_key_pair(cert_path, key_path):
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    except Exception as exc:
        raise CertificateValidationError(
            "TLS certificate and private key could not be loaded together"
        ) from exc
