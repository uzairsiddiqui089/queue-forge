# Deployment

How to run QueueForge on **one server** with Docker Compose, with HTTPS, hardened containers, and a way to update and roll back. It is written for a single host; there is no Kubernetes, no multi-region and no automated deployment pipeline (see "What is not done").

## What was tested, and what was not

| Piece | Status |
|---|---|
| `docker-compose.prod.yml` (production mode, no published ports, Redis password, read-only containers, no capabilities) | **Tested**: started in a separate Compose project, all services healthy, containers inspected (`ReadonlyRootfs`, `CapDrop=ALL`, `no-new-privileges`), API docs return 404, Redis rejects a missing password, rate-limit keys appear in Redis, and a job submitted to the API ran to `succeeded`. |
| HTTPS proxy (`deploy/Caddyfile`, profile `proxy`) | **Not tested.** The Compose file validates (`config`), but the proxy image could not be pulled where this was written, so no certificate was issued and no request went through it. Test it on your server before trusting it. |
| Update and rollback procedure below | Written from how the pieces behave; **not rehearsed** on a real server. |
| Staging, automated deployment (CD) | **Not built.** |

## 1. Prepare the server

- A Linux server with Docker and Compose v2.24.4 or newer (the override uses `!reset`).
- A DNS name pointing at the server, and ports 80 and 443 open (80 is needed to obtain the certificate).
- Nothing else is exposed: the database, Redis and the API publish **no ports** in production mode.

## 2. Configure secrets

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env` and set:

- `POSTGRES_PASSWORD`: a long random value (production mode refuses placeholders and short passwords). Generate with `openssl rand -base64 24`.
- `REDIS_PASSWORD`: another random value.
- `QUEUEFORGE_DOMAIN`: your domain, for example `jobs.example.com`.
- `ENVIRONMENT=production` is set by the override, but keeping it in `.env` too is harmless.

`.env` stays on the server and out of git. These values are visible to anyone with access to the Docker daemon on that machine (`docker inspect`); a secrets manager is the next step up and is not built here.

## 3. Start

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile proxy up -d --build
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
curl https://YOUR_DOMAIN/readyz
```

Create the first admin client. The image does not contain `scripts/`, so feed the script to Python in a one-off container on the Compose network (the key is printed once; copy it now). This was tested on the production override:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm -T --no-deps api \
  python - ops-admin --admin < scripts/create_client.py
```

Use a name without `--admin` for ordinary clients. Create one more admin just for monitoring (`/metrics` needs an admin key).

Turn on backups and, if you need a recovery point of minutes, point-in-time recovery: [operations](operations.md). Set up alerts: [runbooks](runbooks.md).

## 4. Update to a new version

1. **Back up first:** `docker compose --profile tools run --rm dbtools backup` (and note the current git commit, for example in a file or the deployment log).
2. Fetch the new code: `git fetch && git checkout <new tag or commit>`.
3. Rebuild and restart: the `migrate` service runs first and applies new migrations, then the API, workers and maintenance start.

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile proxy up -d --build
```

4. Check `/readyz`, `queueforge_workers_alive`, and the queue lag metric.

Workers receive a stop signal, finish or hand back their job (up to 40 s), and the new ones take over; a job handed back or interrupted runs again (QueueForge is at-least-once).

## 5. Roll back

Migrations only go **forward** in this project: there is no tested downgrade path. So a rollback depends on what the bad release changed:

- **The release did not change the database schema** (no new file in `migrations/versions/`): check out the previous commit and run the same `up -d --build`. Safe and quick.
- **The release added a migration that old code can tolerate** (adding a nullable column or an index, for example): the previous code usually still works against the new schema. Check out the previous commit and rebuild; leave the schema as is.
- **The release changed the schema in a way old code cannot use** (dropping or renaming something): restore the backup taken in step 4 (see the disaster-recovery steps in [operations](operations.md)), then deploy the previous commit. Everything written since the backup is lost, unless you use point-in-time recovery.

Rule for future migrations: make them **backward compatible for one release** (add first, switch the code, remove in a later release), so the first two cases are the only ones you meet. This rule is a convention, not something the tests enforce today.

## What is not done

- **No automated deployment pipeline** (CD): it needs a server and credentials, which this project does not have. The steps above are manual.
- **No staging environment.** To rehearse, run a second copy with a different Compose project name (`-p`) and its own `.env`; restore a backup into it before you try a risky release.
- **Supply chain:** runtime dependencies are audited with `pip-audit`, and the built container image is scanned by Trivy (HIGH and CRITICAL vulnerabilities with an available fix fail CI) on every run. Dependabot opens weekly update pull requests for Python packages, GitHub Actions and the base image; a person merges them. This catches known problems only. Not covered: signing images, publishing an image registry, and GitHub-side settings such as branch protection and secret scanning (these are repository settings, not files, and are not changed here).
- **Secrets are in a `.env` file**, not a secrets manager; there is no key rotation procedure beyond creating a new client and disabling the old one.
- **Single server, single database**: no failover. A server loss means restoring from backups elsewhere.
- Resource limits (memory, CPU) are not set because no sizing data exists beyond the laptop benchmarks in [benchmarking](benchmarking.md).
