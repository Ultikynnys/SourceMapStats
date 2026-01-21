#!/bin/bash
# Script to run Certbot for Nginx on Linux

# Load variables
if [ -f .env ]; then
    while IFS='=' read -r key value; do
        if [[ $key =~ ^# ]] || [[ -z $key ]]; then continue; fi
        value=$(echo "$value" | tr -d '\r')
        export "$key=$value"
    done < .env
else
    echo "ERROR: .env file not found."
    exit 1
fi

WEBROOT="${CERTBOT_WEBROOT:-/usr/share/nginx/html}"
DOMAIN="${DOMAIN_NAME:-localhost}"

# Helper function to enable content in config
uncomment_ssl() {
    sed -i 's/^\s*#\s*listen\s*443/    listen 443/g' /etc/nginx/sites-available/sourcemapstats
    sed -i 's/^\s*#\s*ssl_/    ssl_/g' /etc/nginx/sites-available/sourcemapstats
}

# Helper function to disable content in config (for bootstrapping)
comment_ssl() {
    sed -i 's/^\s*listen\s*443/    # listen 443/g' /etc/nginx/sites-available/sourcemapstats
    sed -i 's/^\s*ssl_/    # ssl_/g' /etc/nginx/sites-available/sourcemapstats
}

echo "Ensuring configurations..."

# Ensure Nginx site is enabled
rm -f /etc/nginx/sites-enabled/default
if [ -f nginx.conf ] && [ ! -f /etc/nginx/sites-available/sourcemapstats ]; then
    cp nginx.conf /etc/nginx/sites-available/sourcemapstats
fi
ln -sf /etc/nginx/sites-available/sourcemapstats /etc/nginx/sites-enabled/

# Ensure webroot privileges
mkdir -p "$WEBROOT"
chown -R www-data:www-data "$WEBROOT"

# Check if certificates already exist
if [ -d "/etc/letsencrypt/live/$DOMAIN" ]; then
    echo "Certificates found for $DOMAIN. refreshing..."
    # Ensure SSL is enabled in config
    uncomment_ssl
    systemctl reload nginx
    certbot certonly --webroot -w "$WEBROOT" -d "$DOMAIN" -m "$CERTBOT_EMAIL" --agree-tos --no-eff-email --force-renewal
else
    echo "No certificates found. Bootstrapping..."
    # Bootstrap: Disable SSL -> Start Nginx -> Get Cert -> Enable SSL
    systemctl stop nginx
    comment_ssl
    systemctl start nginx
    
    certbot certonly --webroot -w "$WEBROOT" -d "$DOMAIN" -m "$CERTBOT_EMAIL" --agree-tos --no-eff-email
    
    if [ $? -eq 0 ]; then
        echo "Certificate obtained. Enabling SSL..."
        uncomment_ssl
        systemctl reload nginx
        echo "Success."
    else
        echo "Certbot failed."
        exit 1
    fi
fi

# Final reload to ensure everything is serving correctly
systemctl reload nginx
echo "Reference: Configured for production."
