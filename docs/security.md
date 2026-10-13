# Security assumptions and design

This is a demonstration project. These notes say what it protects against, what it assumes, and what it does not do.

## Assumptions

- Clients are semi-trusted: they have an API key but may send bad input or try to read other clients' data.
- The network between clients and the API is protected by something else (a TLS-terminating proxy). The API itself speaks plain HTTP.
- PostgreSQL, Redis and the workers run in a trusted network. The local Docker Compose setup binds their ports to `127.0.0.1` only.
- Whoever can read the logs and the database is trusted (job payloads are stored in the database in clear text).

## What is protected, and how

| Risk | Protection | Tested |
|---|---|---|
| Running attacker-supplied code | Only four allow-listed task types exist. Payloads are plain validated data, never executed or shell-evaluated | yes (unknown task type gives 422) |
| Oversized or malformed input | Per-task payload models with length and range limits; unknown fields rejected; `64 KB` request limit checked by header and by bytes received | yes |
| Unauthenticated access | `X-API-Key` required on every `/v1` and `/metrics` call | yes |
| Stolen database | API keys are stored only as SHA-256 hashes. Keys are random (256 bits), so a fast hash is enough; no password-style salting is needed | |
| Reading other clients' jobs | Every query is scoped by `client_id`; another client's job returns `404`, the same as a missing job | yes |
| Privilege misuse | Admin endpoints and `/metrics` need a key whose role is `admin` (`403` otherwise) | yes |
| Abuse and floods | Per-key rate limit (Redis) | yes |
| Secrets in logs | Logs use a fixed allow-list of fields; payloads, headers, query strings and keys are never logged | yes |
| Secrets in error messages | Unknown exceptions store only a generic message in the database and API; the real error goes to the log only | yes |
| Stack traces to clients | Unhandled errors return `{"detail": "Internal server error"}` | yes |
| Secrets in the repository | Config comes from environment variables; only `.env.example` (placeholders) is committed; `.env` is git-ignored | |
| Duplicate side effects | Idempotent handlers and the lease-token fence (see architecture) | yes |

## What it does not do (yet, or at all)

- **No TLS, no key rotation, no key expiry.** To rotate a key, create a new client and disable the old one (`disabled_at`).
- **No per-IP limiting.** Rate limits are per API key, so unauthenticated floods are not limited.
- **Payloads are stored unencrypted** (in the database and its backups) until retention deletes the job: 7 days after success, 30 days after failure by default. Do not put real secrets or personal data in jobs.
- **Exception text in logs.** Stack traces can contain whatever an exception message contains. Protect log access.
- **Redis has no password locally.** In production use a password and TLS, and keep it off public networks.
- **The rate limiter fails open.** During a Redis outage there is no limit. This is a deliberate trade-off (availability over protection).
- **No audit log** of admin actions beyond the normal request log.
- **Dependencies** are not scanned automatically.

## Reporting problems

This is a learning project. If you find a security problem, open an issue on the repository.
