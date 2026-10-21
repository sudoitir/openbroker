#!/usr/bin/env bash
# Regenerates the throwaway certificates the Artemis integration tests use for mutual TLS:
#
#   broker.p12      the broker's key and certificate (CN=localhost, SAN localhost and 127.0.0.1)
#   client.p12      a client's key and certificate (CN=tck-client)
#   truststore.p12  the test authority, which both sides trust
#
# They protect nothing: the password is public, the authority's key is deleted here, and they last
# a hundred years so that no build ever fails on an expired test certificate. Needs only keytool.
set -euo pipefail

cd "$(dirname "$0")"
PASSWORD=openbroker
VALIDITY=36500
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

rm -f broker.p12 client.p12 truststore.p12

keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 -validity "$VALIDITY" \
  -dname "CN=OpenBroker Test CA" -ext bc:c -ext ku=keyCertSign,cRLSign \
  -keystore "$work/ca.p12" -storetype PKCS12 -storepass "$PASSWORD"
keytool -exportcert -alias ca -rfc -keystore "$work/ca.p12" -storepass "$PASSWORD" -file "$work/ca.pem"
keytool -importcert -noprompt -alias ca -file "$work/ca.pem" \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$PASSWORD"

# issue NAME DN EXTENSIONS...: a key and a certificate for DN signed by the authority, in NAME.p12
issue() {
  local name="$1" dn="$2"
  shift 2
  keytool -genkeypair -alias "$name" -keyalg RSA -keysize 2048 -validity "$VALIDITY" \
    -dname "$dn" -keystore "$name.p12" -storetype PKCS12 -storepass "$PASSWORD"
  keytool -certreq -alias "$name" -keystore "$name.p12" -storepass "$PASSWORD" -file "$work/$name.csr"
  keytool -gencert -alias ca -keystore "$work/ca.p12" -storepass "$PASSWORD" -validity "$VALIDITY" \
    -infile "$work/$name.csr" -outfile "$work/$name.pem" -rfc "$@"
  keytool -importcert -noprompt -alias ca -file "$work/ca.pem" -keystore "$name.p12" -storepass "$PASSWORD"
  keytool -importcert -noprompt -alias "$name" -file "$work/$name.pem" -keystore "$name.p12" -storepass "$PASSWORD"
}

issue broker "CN=localhost" -ext san=dns:localhost,ip:127.0.0.1 -ext eku=serverAuth
issue client "CN=tck-client" -ext eku=clientAuth

ls -l broker.p12 client.p12 truststore.p12
