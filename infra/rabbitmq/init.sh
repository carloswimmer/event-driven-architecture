#!/bin/sh
set -eu

RABBITMQ_API="http://${RABBITMQ_HOST}:15672/api"
AUTH="${RABBITMQ_USER}:${RABBITMQ_PASS}"
MAX_ATTEMPTS=30

wait_for_management_api() {
  attempt=1
  echo "Waiting for RabbitMQ management API..."
  while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
    if curl -sf -u "$AUTH" "${RABBITMQ_API}/overview" > /dev/null; then
      echo "Management API is ready."
      return 0
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
  echo "RabbitMQ management API did not become ready in time." >&2
  exit 1
}

import_topology() {
  echo "Importing RabbitMQ topology (vhost, exchanges, queues, bindings)..."
  curl -sf -u "$AUTH" \
    -H "content-type: application/json" \
    -X POST "${RABBITMQ_API}/definitions" \
    --data-binary "@/init/definitions.json"
}

provision_app_user() {
  echo "Provisioning application user '${RABBITMQ_APP_USER}' on vhost 'eda'..."

  curl -sf -u "$AUTH" \
    -H "content-type: application/json" \
    -X PUT "${RABBITMQ_API}/users/${RABBITMQ_APP_USER}" \
    -d "{\"password\":\"${RABBITMQ_APP_PASS}\",\"tags\":\"\"}"

  curl -sf -u "$AUTH" \
    -H "content-type: application/json" \
    -X PUT "${RABBITMQ_API}/permissions/eda/${RABBITMQ_APP_USER}" \
    -d '{"configure":".*","write":".*","read":".*"}'
}

wait_for_management_api
import_topology
provision_app_user

echo "RabbitMQ initialization complete."
