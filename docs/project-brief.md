# QueueForge: Project Brief

**Project:** QueueForge, a distributed job processing platform
**Primary language:** Python
**API framework:** FastAPI
**Database:** PostgreSQL
**Coordination:** Redis, only where it gives a clear benefit

**Goal:** a portfolio-quality backend project that demonstrates designing, implementing, testing, debugging, documenting and explaining a reliable backend system.

## 1. Purpose

QueueForge demonstrates:

- Backend API design with Python and FastAPI
- Database schema design and transaction management
- Distributed systems fundamentals
- Concurrent worker processing
- Failure handling and recovery
- Idempotency and duplicate prevention
- Retry strategies and dead-letter handling
- Authentication and security fundamentals
- Automated testing
- Docker and CI/CD fundamentals
- Observability and performance measurement
- System design reasoning
- Clear technical documentation

The project is built incrementally. Correctness, reliability and understanding take priority over unnecessary complexity.

## 2. What we are building

A client submits a job through an HTTP API. The system validates the request and stores the job durably. One or more worker processes claim eligible jobs, execute supported task types and persist their outcomes. Clients can retrieve job status, results and failure information.

Failed jobs are retried a bounded number of times with a delay strategy, distinguishing temporary from permanent failures. If a worker crashes mid-job, a documented recovery mechanism takes over. Jobs that exhaust their attempts move to a dead-letter state for inspection.

Only a small set of safe demonstration task types is supported. The system never executes arbitrary code or shell commands supplied by API clients.

## 3. Technology stack

- **Python and FastAPI:** typed, modular code with validated request and response models.
- **PostgreSQL:** the durable source of truth for jobs, status transitions, attempts and other state. Transactions protect important state changes.
- **Redis:** used only where it gives a clear benefit (for example rate limiting). It is never the only place important job state lives.
- **Workers:** separate processes from the API.
- **pytest:** unit and integration tests, including competing workers and recovery.
- **Docker and Docker Compose:** a reproducible local environment.
- **GitHub Actions:** CI for tests, linting and type checking.
- **Observability:** structured logging, health and readiness endpoints, metrics and troubleshooting docs.

## 4. Core features (implemented incrementally)

1. Submit a job through a validated API request.
2. Retrieve a job and inspect its status.
3. Explicit lifecycle states: queued, running, succeeded, failed, dead-lettered.
4. Legal state transitions, enforced.
5. Idempotency keys with a documented scope and retention period.
6. Safe work claiming with multiple workers.
7. Execution attempt tracking.
8. Bounded retries with exponential backoff and jitter.
9. Retryable versus permanent error classification.
10. Recovery of abandoned jobs after worker crashes.
11. A documented lease, timeout or equivalent recovery mechanism.
12. Useful failure details without exposing secrets.
13. Health checks and structured logs.
14. Integration and failure-path tests.
15. Repeatable performance testing scripts.
16. Documentation of architecture, guarantees, limitations and trade-offs.

## 5. Reliability principles (mandatory)

- **Delivery guarantees:** no claim of exactly-once execution. Design for at-least-once processing and idempotent handlers. Document the difference between accepting, persisting, claiming, executing and recording a job, and what happens if a worker crashes between any two of these steps.
- **Concurrency:** define how workers claim jobs safely, explain why it works under load, and test competing workers.
- **Retries:** a maximum attempt count and a defined delay strategy; no infinite retry loops.
- **Worker recovery:** explain how abandoned jobs are identified, ensure they cannot stay stuck forever, and explain the possibility of duplicate execution during recovery.
- **Security:** validate inputs, authenticate and authorize, protect secrets, avoid sensitive data in logs, never execute client-supplied code.
- **Simplicity:** no unnecessary microservices or Kubernetes. Explain every important dependency.
- **Honesty:** never invent test results, performance numbers, production usage or completed features. Report benchmarks only after running them.

## 6. Milestones

| # | Milestone | Scope |
|---|---|---|
| 0 | Architecture and planning | Requirements, architecture, flows, states and transitions, PostgreSQL versus Redis, risks, repository structure, tools, acceptance criteria, trade-offs. No application code until the plan is approved. |
| 1 | Repository foundation | Python environment, configuration, Docker Compose, basic FastAPI app, health endpoint, pytest, linting and type checking, initial GitHub Actions workflow, `.gitignore` and env example. |
| 2 | Durable job API | Schema and migrations, submission and retrieval endpoints, validation, explicit states, transaction-safe persistence, error responses, tests. |
| 3 | Worker execution | Separate worker, safe claiming, allow-listed handlers, attempt tracking, result persistence, concurrency controls, competing-worker tests. |
| 4 | Failure handling and recovery | Idempotency, error classification, bounded retries, backoff with jitter, leases, dead-lettering, crash and restart tests. |
| 5 | Security and observability | Authentication and authorization, input validation, secrets, safe and structured logging, health and readiness, metrics, troubleshooting docs. |
| 6 | Performance testing | A reproducible benchmark: throughput, queue wait, end-to-end latency (p50/p95/p99), error rate, different worker counts. Document environment, workload, exact commands and actual results only. |
| 7 | Portfolio readiness | README, architecture docs and diagram, API examples, ADRs, operations guide, benchmarking guide, limitations, demo walkthrough, interview notes. |

## 7. Definition of done for every milestone

1. State the acceptance criteria.
2. Implement a small, understandable set of changes.
3. Add appropriate tests.
4. Run the tests and show the exact commands.
5. Investigate failures instead of hiding them.
6. Explain the important implementation decisions.
7. Update the documentation.
8. State what should be understood before continuing.
9. Wait for confirmation before the next milestone.

## 8. Expected documentation

- `README.md`: overview, features, prerequisites, setup, API examples, testing, limitations
- `docs/architecture.md`: components, request flow, job lifecycle, data ownership, concurrency, failure recovery
- `docs/decisions/`: architecture decision records
- `docs/operations.md`: configuration, health checks, logs, retry behaviour, recovery, troubleshooting
- `docs/benchmarking.md`: environment, workload, commands, actual measurements, limitations
- `docs/interview-notes.md`: design rationale, alternatives, failure scenarios, bottlenecks, scaling options

## 9. Working agreement

Work one milestone at a time, explain code and trade-offs in plain English, give exact file paths and commands, write typed and tested code, keep scope appropriate for one developer, never put secrets in source code, never claim exactly-once execution, and never invent results. The aim is to learn while building, not to generate code.
