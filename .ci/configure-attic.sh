#!/usr/bin/env bash
# Configure the Attic cache as a pull-only substituter. The cache is
# public, so pulling needs no credentials on any event; push credentials
# are obtained separately by .ci/attic-login-push.sh right before a push.
set -euo pipefail
: "${ATTIC_SERVER:=https://attic.jeiang.dev/}"
: "${ATTIC_CACHE:=default}"
export PATH=$HOME/.nix-profile/bin:$PATH # FIXME

attic login --set-default ci "$ATTIC_SERVER"
attic use "$ATTIC_CACHE"
echo "ATTIC_SERVER=$ATTIC_SERVER" >>"$GITHUB_ENV"
echo "ATTIC_CACHE=$ATTIC_CACHE" >>"$GITHUB_ENV"
