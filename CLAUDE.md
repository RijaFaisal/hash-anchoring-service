# hash-anchor

A document hash anchoring and verification service. Clients submit a document;
the backend hashes it, records it, and anchors the hash on-chain via a
Solidity contract, all through an async, at-least-once-delivery pipeline.

## Architecture

1. **Submit**: Client `POST`s a document to the Spring Boot API. The API
   computes SHA-256, then in a single DB transaction: saves the record as
   `PENDING` and inserts an `outbox_events` row (transactional outbox
   pattern — guarantees the event is never lost or published without the
   record existing). Returns `202 Accepted`.
2. **Relay**: A scheduled outbox relay polls unpublished `outbox_events` rows,
   publishes them to the Kafka topic `records.submitted`, and marks them
   published.
3. **Anchor**: An anchoring consumer reads from `records.submitted`:
   - Skips processing if the record is already `ANCHORED` (idempotency —
     Kafka is at-least-once, so consumers must tolerate redelivery).
   - Calls the `HashAnchor` Solidity contract via web3j, waits for the
     transaction receipt and confirmations, then updates the record to
     `ANCHORED` with `tx_hash` and `block_number`.
   - On contract/network failure: retries with exponential backoff, then
     routes to the dead-letter topic `records.submitted.DLT` and marks the
     record `FAILED`.
   - Special case: if the contract reverts with `"Already anchored"` (e.g. a
     retried event that actually succeeded on-chain before the ack was
     recorded), call `verify()` instead of failing, and mark `ANCHORED` from
     that result.
4. **Verify**: `POST /api/verify` recomputes the SHA-256 of a submitted
   document and checks it directly against the `HashAnchor` contract's
   `verify()` — no dependency on the local DB record being present or in any
   particular state.

## Stack

- Java 21, Spring Boot 3.x, Gradle (Kotlin DSL)
- PostgreSQL + Flyway for migrations
- Spring Data JPA
- Spring Kafka, Kafka in KRaft mode (no ZooKeeper)
- web3j for contract calls
- Solidity 0.8.x, Hardhat for contract dev/test, local Hardhat node in dev,
  Polygon Amoy testnet only for the final deployment step
- JUnit 5 + Testcontainers (Postgres + Kafka) for backend integration tests
- Docker Compose to run Postgres + Kafka locally

## Repo layout

```
hash-anchor/
  contracts/   Hardhat project: HashAnchor.sol + tests
  backend/     Spring Boot app
    src/main/java/com/hashanchor/
      api/          controllers, request/response DTOs
      domain/       entities, status enums, core logic
      persistence/  Spring Data repositories
      messaging/    outbox relay, Kafka producer/consumer
      blockchain/   web3j client, contract wrapper, tx/confirmation logic
      config/       Spring configuration classes
  docker-compose.yml
  README.md
```

## Rules

- **Never commit secrets.** The blockchain private key (Hardhat dev key,
  later the Amoy deployer key) is read from an environment variable only —
  never hardcoded, never checked in. `.env` files are gitignored.
- **Idempotency matters everywhere on the consumer side.** Kafka delivery is
  at-least-once; every consumer handler must be safe to run twice on the same
  event (check current status before acting).
- **The outbox pattern is non-negotiable for the submit path.** Never publish
  to Kafka directly inside the request-handling transaction path without the
  outbox row backing it — that's what makes the "save record + emit event"
  step atomic.
- **Status lifecycle**: `PENDING → ANCHORED` (happy path) or
  `PENDING → FAILED` (after retries exhausted, routed to DLT). `FAILED` is
  not necessarily terminal in the domain sense, but is treated as terminal by
  the automated retry pipeline (no automatic re-drive from `FAILED`).
- Contract owner-only functions must use an explicit access-control check
  (e.g. `onlyOwner`), not rely on convention.
- Local development targets a local Hardhat node. Polygon Amoy is only
  touched at the very end of the project, deliberately, once the pipeline is
  proven locally.

## Working style

- The user knows Python and some Solidity, and is new to Java/Spring Boot.
  Briefly explain non-obvious Java/Spring/Gradle decisions as they come up
  (e.g. why a bean is scoped a certain way, what an annotation does) — no
  need to explain Solidity/Hardhat basics.
- Work in phases; stop after each phase for review unless told otherwise.
