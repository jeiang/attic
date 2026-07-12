# OIDC Authentication

Attic can exchange a verified OIDC ID token for a short-lived Attic token. The
resulting token uses the normal Attic permission model, so Nix and existing
clients continue to work without any API changes. Tokens created by
`atticadm make-token` remain supported.

Configure every trusted issuer explicitly. Rules are deny-by-default: all
claims in a rule must match. A scalar rule value matches an identical scalar
claim or an item in an array claim. Permission fields use the existing JWT
names (`r` for pull, `w` for push, `d` for delete, and `cc`, `cr`, `cq`, `cd`
for cache administration).

```toml
[[oidc.providers]]
name = "pocketid"
mode = "authorization-code-pkce"
issuer = "https://id.example.com"
audience = "your-pocket-id-client-id"
jwks-url = "https://id.example.com/.well-known/jwks.json"
authorization-endpoint = "https://id.example.com/authorize"
token-endpoint = "https://id.example.com/api/oidc/token"
scopes = ["openid", "profile", "groups"]

[[oidc.providers.rules]]
claims = { groups = "attic-users" }
caches = { "dev-cache" = { r = 1, w = 1 } }
```

Create a Pocket ID public client with PKCE enabled and register
`http://127.0.0.1:*/callback` as its callback. Then log in with:

```console
$ attic login --set-default home https://attic.example/ --oidc pocketid
```

For GitHub Actions, configure GitHub's JWKS endpoint and only grant access to
an immutable repository ID and protected ref:

```toml
[[oidc.providers]]
name = "github-actions"
mode = "github-actions"
issuer = "https://token.actions.githubusercontent.com"
audience = "https://attic.example/"
jwks-url = "https://token.actions.githubusercontent.com/.well-known/jwks"

[[oidc.providers.rules]]
claims = { repository_id = "123456789", ref = "refs/heads/main", ref_protected = "true" }
caches = { "project-cache" = { r = 1, w = 1 } }
```

The workflow must grant `id-token: write`; no `ATTIC_TOKEN` secret is needed:

```yaml
permissions:
  contents: read
  id-token: write

steps:
  - uses: actions/checkout@v7
  - run: attic login --set-default ci "$ATTIC_SERVER" --oidc github-actions
  - run: attic use project-cache
  - run: attic push project-cache result
```

The exchanged token defaults to 12 hours and can be changed with
`token-validity`. Remove a matching rule to stop future exchanges; already
issued exchange tokens remain usable until they expire.
