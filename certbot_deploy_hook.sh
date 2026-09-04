#!/bin/sh
# Deploy hook for certbot: reload nginx after a successful renewal so it
# serves the freshly-issued certificate instead of the stale one cached in
# memory. Installed at /etc/letsencrypt/renewal-hooks/deploy/00-reload-nginx.sh
# by run_certbot.sh.
set -eu

nginx -t -q && systemctl reload nginx
