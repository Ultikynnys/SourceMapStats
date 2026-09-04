#!/usr/bin/env bash
set -euo pipefail

# ----------------------------------------
# Create and activate a Python virtualenv
# ----------------------------------------
if [ ! -d venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

echo
echo "Activating virtual environment..."
# shellcheck disable=SC1091
source venv/bin/activate

# ----------------------------------------
# Upgrade pip and install dependencies
# ----------------------------------------
echo
echo "Upgrading pip and installing requirements..."
pip install --upgrade pip
if [ -f requirements.txt ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found! Please add one before proceeding."
    exit 1
fi

# ----------------------------------------
# Validate production TLS before starting
# ----------------------------------------
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

REQUIRE_VALID_CERTS="${REQUIRE_VALID_CERTS:-true}"
if [[ ! "$REQUIRE_VALID_CERTS" =~ ^(0|false|no|off)$ ]]; then
    DOMAIN="${DOMAIN_NAME:-tf2stats.r60d.xyz}"
    TLS_PORT="${TLS_VERIFY_PORT:-443}"
    CERT_FILE="${TLS_CERT_FILE:-/etc/letsencrypt/live/$DOMAIN/fullchain.pem}"
    KEY_FILE="${TLS_KEY_FILE:-/etc/letsencrypt/live/$DOMAIN/privkey.pem}"

    echo
    echo "Validating TLS certificate for $DOMAIN..."

    if ! command -v openssl >/dev/null 2>&1; then
        echo "ERROR: openssl is required to validate TLS certificates."
        exit 1
    fi

    if [ ! -r "$CERT_FILE" ]; then
        echo "ERROR: TLS certificate file is missing or unreadable: $CERT_FILE"
        exit 1
    fi

    if [ ! -r "$KEY_FILE" ]; then
        echo "ERROR: TLS private key file is missing or unreadable: $KEY_FILE"
        exit 1
    fi

    if ! openssl x509 -in "$CERT_FILE" -noout >/dev/null 2>&1; then
        echo "ERROR: TLS certificate cannot be parsed: $CERT_FILE"
        exit 1
    fi

    if ! openssl x509 -in "$CERT_FILE" -noout -checkend 0 >/dev/null; then
        echo "ERROR: TLS certificate is expired: $CERT_FILE"
        exit 1
    fi

    CERT_PUBLIC_KEY_HASH=$(openssl x509 -in "$CERT_FILE" -noout -pubkey | openssl sha256)
    KEY_PUBLIC_KEY_HASH=$(openssl pkey -in "$KEY_FILE" -pubout 2>/dev/null | openssl sha256)
    if [ -z "$KEY_PUBLIC_KEY_HASH" ] || [ "$CERT_PUBLIC_KEY_HASH" != "$KEY_PUBLIC_KEY_HASH" ]; then
        echo "ERROR: TLS certificate and private key do not match."
        exit 1
    fi

    if ! openssl s_client \
        -connect "$DOMAIN:$TLS_PORT" \
        -servername "$DOMAIN" \
        -verify_hostname "$DOMAIN" \
        -verify_return_error \
        -brief </dev/null >/dev/null 2>&1; then
        echo "ERROR: Live TLS validation failed for $DOMAIN:$TLS_PORT"
        echo "ERROR: Refusing to start app while browsers would reject HTTPS."
        exit 1
    fi

    echo "TLS certificate validation passed."
fi

# ----------------------------------------
# Determine local IPv4 address
# ----------------------------------------
LOCAL_IP=$(hostname -I | awk '{print $1}')
if [ -z "$LOCAL_IP" ]; then
    LOCAL_IP="127.0.0.1"
fi

# ----------------------------------------
# Run the Flask app via Waitress
# ----------------------------------------
echo
echo "Starting the Flask app on http://$LOCAL_IP:5000"
exec python app.py
