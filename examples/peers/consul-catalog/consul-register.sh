#!/bin/sh
# consul-register.sh
#
# Registers proxy-a, proxy-b, and proxy-c as loki-vl-proxy service instances
# in Consul via the HTTP API.  Each registration includes a health check that
# polls /ready on the proxy's container address.
#
# This script runs once at startup (consul-setup service) and exits.
# Consul tracks health going forward; ?passing=true in the catalog query
# automatically excludes unhealthy instances from the peer list.

set -e

CONSUL=http://consul:8500

register() {
  local id="$1"
  local name="loki-vl-proxy"
  local address="$2"
  local port=3100

  echo "Registering ${id} at ${address}:${port}..."

  curl -sf -XPUT "${CONSUL}/v1/agent/service/register" \
    -H "Content-Type: application/json" \
    -d "{
      \"ID\":      \"${id}\",
      \"Name\":    \"${name}\",
      \"Address\": \"${address}\",
      \"Port\":    ${port},
      \"Tags\":    [\"proxy\", \"loki-vl-proxy\"],
      \"Check\": {
        \"HTTP\":                        \"http://${address}:${port}/ready\",
        \"Interval\":                    \"10s\",
        \"Timeout\":                     \"5s\",
        \"DeregisterCriticalServiceAfter\": \"60s\"
      }
    }"

  echo "Registered ${id}."
}

register "proxy-a" "proxy-a"
register "proxy-b" "proxy-b"
register "proxy-c" "proxy-c"

echo "All proxies registered in Consul."
