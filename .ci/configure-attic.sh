#!/usr/bin/env bash
set -euo pipefail
: "${ATTIC_SERVER:=https://attic.jeiang.dev/}"
: "${ATTIC_CACHE:=default}"
export PATH=$HOME/.nix-profile/bin:$PATH # FIXME

providers=$(curl -fsSL "${ATTIC_SERVER%/}/_api/v1/auth/oidc/providers")
provider=$(jq -r 'first(.providers[] | select(.mode == "github-actions")) | .name' <<<"$providers")
audience=$(jq -r 'first(.providers[] | select(.mode == "github-actions")) | .audience' <<<"$providers")

id_token=$(curl -fsSL -G --data-urlencode "audience=$audience" \
  -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
  "$ACTIONS_ID_TOKEN_REQUEST_URL" | jq -r .value)

token=$(jq -n --arg provider "$provider" --arg id_token "$id_token" \
    '{provider: $provider, id_token: $id_token}' \
  | curl -fsSL -X POST -H 'Content-Type: application/json' --data @- \
    "${ATTIC_SERVER%/}/_api/v1/auth/oidc/exchange" \
  | jq -r .access_token)

attic login --set-default ci "$ATTIC_SERVER" "$token"
attic use "$ATTIC_CACHE"
echo "ATTIC_CACHE=$ATTIC_CACHE" >>"$GITHUB_ENV"
