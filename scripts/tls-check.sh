#!/bin/sh
# TLS terminates at nginx. OSD1 stays on HTTP inside the compose network.
set -eu
cd "$(dirname "$0")/.."

export OSD1_API_TOKEN="${OSD1_API_TOKEN:-tls-secret-token}"
tls=$(mktemp -d)
export OSD1_TLS_DIR="$tls"
compose() {
  docker compose -f deploy/compose.yml -f deploy/compose.tls.yml "$@"
}
cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tls"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 \
  -keyout "$tls/key.pem" -out "$tls/cert.pem" \
  -days 1 -nodes -subj "/CN=localhost" >/dev/null 2>&1
chmod 644 "$tls/key.pem" "$tls/cert.pem"

echo "starting osd1 and the tls proxy"
compose up -d --build

i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS -k https://127.0.0.1:8443/health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
if ! curl -fsS -k https://127.0.0.1:8443/health >/dev/null 2>&1; then
  echo "tls proxy did not become healthy" >&2
  compose logs >&2 || true
  exit 1
fi

health=$(curl -fsS -k https://127.0.0.1:8443/health)
echo "$health"
echo "$health" | grep -q '{"ok":true}'

missing=$(curl -sk -o /dev/null -w '%{http_code}' -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  "https://127.0.0.1:8443/accounts/local/d1/database/osd1/query")
echo "missing_token_status $missing"
test "$missing" = "401"

wrong=$(curl -sk -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer wrong-token" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  "https://127.0.0.1:8443/accounts/local/d1/database/osd1/query")
echo "wrong_token_status $wrong"
test "$wrong" = "401"

body=$(curl -fsS -k -H "Authorization: Bearer $OSD1_API_TOKEN" -H "content-type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' \
  "https://127.0.0.1:8443/accounts/local/d1/database/osd1/query")
echo "$body"
echo "$body" | grep -q '"n":1'
echo "tls ok"
