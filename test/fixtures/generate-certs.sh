#!/usr/bin/env bash
# Generate the throwaway TLS material the live fixtures need.
#
# One CA and one server certificate, shared by every engine. The original
# ad-hoc fixtures used a separate CA per engine, which bought nothing: the
# tests require verified TLS against a known root, not distinct roots.
#
# The certificate carries CN=localhost with SAN DNS:localhost and
# IP:127.0.0.1, because the DSNs connect to 127.0.0.1 and Go verifies the IP
# SAN, not the CN. A certificate with only a CN, or only a DNS SAN, fails
# verify-full against 127.0.0.1 — that exact mismatch cost a debugging cycle
# when the ClickHouse fixture carried an IP-only SAN.
#
# The CA is named ca.pem, not ca.crt, and that is not cosmetic: the SQL Server
# driver dispatches on file extension and rejects .crt outright with
# "certificate type .crt is not supported". The other engines accept any name.
#
# This material is deliberately worthless: it protects nothing, expires
# quickly, and exists only so the tests exercise the real verified-TLS code
# paths rather than a plaintext shortcut. Never reuse it anywhere.
set -euo pipefail

directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
mkdir -p "$directory"
cd "$directory"

if [ -f server.crt ] && [ -f server.key ] && [ -f ca.pem ]; then
  echo "certificates already present in $directory (delete them to regenerate)"
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout ca.key -out ca.pem \
  -subj "/CN=dmtx-test-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout server.key -out server.csr \
  -subj "/CN=localhost" 2>/dev/null

cat > server.ext <<'EXT'
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EXT

openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 -extfile server.ext 2>/dev/null

rm -f server.csr server.ext ca.srl

# World-readable on purpose. These files are bind-mounted into containers that
# run as different, engine-specific users, and bind-mount ownership behaves
# differently on Docker Desktop than on a Linux CI runner. Restrictive modes
# work on one and break on the other; for throwaway material the portable
# choice is the right one. PostgreSQL is the exception and re-secures its own
# copy at startup, because it refuses a group- or world-readable key.
chmod 644 ca.pem server.crt server.key ca.key

echo "wrote CA and server certificate to $directory"
openssl x509 -in server.crt -noout -subject -ext subjectAltName
