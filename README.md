# hash-anchor

A document hash anchoring and verification service. Clients submit a
document; the backend hashes it (SHA-256), records it, and anchors the hash
on-chain through a Solidity contract. Anyone can later upload a file and
prove, against the chain itself, that this exact content was anchored and
when.

Anchoring runs through an asynchronous, at-least-once pipeline
(Postgres transactional outbox → Kafka → anchoring consumer → contract), built
so that crashes, duplicate events, reverts and timeouts never cause a lost
anchor, a double anchor, or a record that lies about what's on-chain.

- [Architecture](#architecture)
- [Record status lifecycle](#record-status-lifecycle)
- [Delivery guarantees and idempotency](#delivery-guarantees-and-idempotency)
- [Failure handling](#failure-handling)
- [Smart contract](#smart-contract)
- [Data model](#data-model)
- [HTTP API](#http-api)
- [Kafka topics](#kafka-topics)
- [Configuration](#configuration)
- [Running locally](#running-locally)
- [Tests](#tests)
- [Repository layout](#repository-layout)
- [Known tradeoffs](#known-tradeoffs)
- [Tech stack](#tech-stack)

---

## Architecture

```mermaid
flowchart LR
    client([Client])

    subgraph backend[Spring Boot backend]
        api[RecordController<br/>VerifyController]
        relay[OutboxRelay<br/>every 5s]
        consumer[AnchoringConsumer<br/>+ AnchoringService]
        recovery[StaleAnchoringRecovery<br/>every 60s]
    end

    subgraph pg[PostgreSQL]
        records[(records)]
        outbox[(outbox_events)]
    end

    subgraph kafka[Kafka - KRaft]
        topic[[records.submitted]]
        dlt[[records.submitted.DLT]]
    end

    chain[(HashAnchor contract<br/>Hardhat / Polygon Amoy)]

    client -- "POST /api/records" --> api
    api -- "1 tx: record + outbox row" --> records & outbox
    relay -- "poll unpublished" --> outbox
    relay -- publish --> topic
    topic --> consumer
    consumer -- "verify() / anchor()" --> chain
    consumer -- "status, tx_hash, block" --> records
    consumer -- "retries exhausted" --> dlt
    recovery -- "stuck ANCHORING" --> records
    recovery -- "verify()" --> chain
    recovery -- "re-queue" --> outbox
    client -- "POST /api/verify" --> api
    api -- "verify() — source of truth" --> chain
```

### 1. Submit — `POST /api/records`

The API computes the document's SHA-256, then **in a single database
transaction** saves a `records` row as `PENDING` and inserts an
`outbox_events` row describing a `RecordSubmitted` event. It returns
`202 Accepted` immediately; anchoring happens in the background.

This is the **transactional outbox pattern**: the request path never talks to
Kafka directly. Because the record and its event are written atomically, it's
impossible to have a record with no event (it would never be anchored) or an
event with no record.

### 2. Relay — `OutboxRelay`

A scheduled job (every 5 seconds by default, `hashanchor.outbox.poll-interval`) reads unpublished `outbox_events` rows
oldest-first, publishes each to the Kafka topic `records.submitted` (keyed by
record id, waiting for the broker's ack), and then marks the row published.

Publishing and marking are **two separate operations** — there's no
transaction spanning Postgres and Kafka. If the process crashes, or the DB
write fails, after Kafka acks but before the row is marked, the row is
published again on the next run. The same event can therefore reach the topic
twice. That's why delivery is **at-least-once**, and why the consumer must be
idempotent (see below).

### 3. Anchor — `AnchoringConsumer` + `AnchoringService`

A `@KafkaListener` consumes `records.submitted` and makes one anchoring
attempt per delivery:

1. **Load the record.** If it's already `ANCHORED` (duplicate event) or
   `FAILED` (terminal for the automated pipeline), skip it. `PENDING` and
   `ANCHORING` are both processed — `ANCHORING` means a previous attempt
   started and didn't finish, so it's resumed from the top.
2. **Mark `ANCHORING`** and increment `attempts` before touching the chain.
3. **`verify()` first.** If the hash is already on-chain (e.g. an earlier
   attempt's transaction landed but the DB update didn't), mark the record
   `ANCHORED` with the block number `verify()` returns — no new transaction.
   The `tx_hash` is recovered from the contract's `HashAnchored` event log in
   that block.
4. **Otherwise `anchor()`** via web3j and wait for the receipt (bounded by
   `receipt-timeout`, 30s). On success, save `tx_hash` and `block_number`
   and mark `ANCHORED`.
5. **On any `anchor()` failure** — revert, receipt timeout, RPC error — call
   `verify()` again. If the hash is now anchored, it's a success (e.g. a
   concurrent or earlier transaction won). If it still isn't, it's a genuine
   failure: save `last_error` and throw, which hands control to Spring
   Kafka's retry logic.

No database transaction is held open across chain calls: each status update
is its own short transaction.

**Never branch on revert reason strings.** RPC providers and client layers
don't reliably preserve them. The contract's `require(..., "Already
anchored")` message exists for humans reading a block explorer;
the backend's only source of truth is `verify()`.

### 4. Retries and dead-lettering — `KafkaConsumerConfig`

When the listener throws, Spring Kafka's `DefaultErrorHandler` redelivers the
same message after an exponential backoff of **1s, 2s, 4s, 8s** (five
attempts in total). When retries are exhausted it:

1. publishes the event to **`records.submitted.DLT`** (with headers recording
   the original topic/partition/offset and the exception), then
2. marks the record **`FAILED`** with `last_error`,

and commits the offset so the partition moves on. If marking `FAILED` itself
fails, the offset isn't committed and the whole cycle repeats — possibly a
duplicate DLT entry, but never a `FAILED` record without one.

Errors that retrying can't fix (event references a non-existent record,
malformed JSON) skip the backoff and go straight to the DLT.

### 5. Stale-`ANCHORING` recovery — `StaleAnchoringRecovery`

Usually a crash mid-attempt needs no help: the offset wasn't committed, so
Kafka redelivers the event on restart and the consumer resumes the
`ANCHORING` record. This job covers the cases where that doesn't happen (e.g.
the event was dead-lettered but the process died before marking `FAILED`).

Every 60s it looks for records that have been in `ANCHORING` with no update
for longer than `stale-after` (5 minutes — well beyond one full retry cycle,
so live work is never mistaken for stuck). For each one it calls `verify()`:

- **anchored on-chain** → mark `ANCHORED` (with `tx_hash` from the event log);
- **not anchored** → reset to `PENDING` and insert a fresh outbox event, in
  one transaction, so the record goes back through the normal pipeline.

It never sends `anchor()` itself. A JPA `@Version` column on `records` makes
concurrent writes from this job and the consumer fail loudly instead of
overwriting each other.

### 6. Verify — `POST /api/verify`

Recomputes the uploaded file's SHA-256 and asks the **contract** whether it's
anchored. If so, it adds the block's timestamp and the anchoring transaction
hash (from the event log). The database is only consulted afterwards, to
attach the matching `recordId` if we have one — and if that lookup fails, the
chain's answer is still returned. A document the database thinks is anchored
but the chain doesn't know about is reported as **not anchored**.

---

## Record status lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING: POST /api/records
    PENDING --> ANCHORING: consumer starts an attempt
    ANCHORING --> ANCHORED: anchor() succeeded,<br/>or verify() shows it on-chain
    ANCHORING --> ANCHORING: attempt failed → backoff → retry
    ANCHORING --> FAILED: retries exhausted → DLT
    ANCHORING --> PENDING: stale recovery,<br/>not on-chain → re-queued
    ANCHORED --> [*]
    FAILED --> [*]
```

| Status | Meaning |
|---|---|
| `PENDING` | Accepted and queued (via the outbox); no attempt in progress. |
| `ANCHORING` | The consumer has started an attempt and not finished it. |
| `ANCHORED` | The hash is on-chain; `block_number` set, `tx_hash` set when known. |
| `FAILED` | Retries exhausted; event sent to the DLT; `last_error` explains why. Treated as terminal by the automated pipeline — nothing re-drives it automatically. |

---

## Delivery guarantees and idempotency

Kafka delivery here is at-least-once, from two sources: the relay can publish
an outbox row twice, and the consumer can be redelivered a message it already
partly processed (crash before offset commit, retries). Every step of the
consumer is safe to repeat:

| Situation | What happens |
|---|---|
| Duplicate event, record already `ANCHORED` | Skipped without touching the chain. |
| Duplicate event, record still `PENDING`/`ANCHORING` but hash already on-chain | `verify()`-first catches it; marked `ANCHORED`, no new transaction. |
| Two `anchor()` transactions for the same hash | The contract rejects the second; the follow-up `verify()` resolves it as `ANCHORED`. |
| Crash mid-attempt | Offset not committed → redelivered → `ANCHORING` record resumed. |
| Record stuck in `ANCHORING` with no redelivery | Stale recovery re-checks the chain and either marks it `ANCHORED` or re-queues it. |
| Consumer and recovery job write the same row concurrently | `@Version` optimistic locking rejects the stale write. |

The contract is the final guard: it can never hold two anchors for one hash.

---

## Failure handling

| Failure | Handling |
|---|---|
| `anchor()` reverts | `verify()`; anchored → success, otherwise retry. |
| Receipt not seen within 30s | Same as a revert: `verify()`, then success or retry. |
| RPC / node unreachable | Exception → retry with backoff → eventually DLT + `FAILED`. |
| Signer out of funds, other persistent errors | Retries fail → DLT + `FAILED` with `last_error`. |
| Event for a non-existent record, malformed payload | Not retried; straight to DLT. |
| Kafka unavailable at relay time | Outbox row stays unpublished; retried on the next relay run. |
| Chain unavailable during `POST /api/verify` | `503 Service Unavailable` — never a false "not anchored". |

---

## Smart contract

`contracts/contracts/HashAnchor.sol` (Solidity 0.8.x):

```solidity
function anchor(bytes32 docHash) external;                  // owner only
function verify(bytes32 docHash) external view returns (bool, uint64);  // (anchored, blockNumber)
event HashAnchored(bytes32 indexed docHash, uint64 blockNumber);
```

- `anchor` is restricted to the deploying account (`owner`, immutable) and
  reverts if the hash is already anchored — each hash can be anchored exactly
  once, at the block it first landed in.
- `verify` is a free `view` call returning whether the hash is anchored and in
  which block.
- `HashAnchored` is indexed on `docHash`, which lets the backend find the
  anchoring transaction for a given hash with a narrow `eth_getLogs` query.

The backend calls the contract through a web3j Java wrapper generated at
build time from the Hardhat artifact (`generateContractWrappers` Gradle task).

---

## Data model

Schema is owned by Flyway (`backend/src/main/resources/db/migration`);
Hibernate only validates against it.

**`records`**

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | |
| `document_hash` | `varchar(66)` | `0x`-prefixed SHA-256; indexed; not unique (the same document may be submitted twice) |
| `status` | `varchar(20)` | `PENDING` / `ANCHORING` / `ANCHORED` / `FAILED` |
| `tx_hash` | `varchar(66)` | Anchoring transaction, when known |
| `block_number` | `bigint` | Block the hash was anchored in |
| `attempts` | `int` | Anchoring attempts started |
| `last_error` | `text` | Most recent failure |
| `version` | `bigint` | Optimistic-lock counter |
| `created_at`, `updated_at` | `timestamptz` | |

**`outbox_events`**

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | |
| `record_id` | `uuid` FK → `records` | Also the Kafka message key |
| `event_type` | `varchar(50)` | `RecordSubmitted` |
| `payload` | `jsonb` | `{"recordId": "...", "documentHash": "0x..."}` |
| `published` | `boolean` | Partial index on unpublished rows |
| `created_at`, `published_at` | `timestamptz` | |

---

## HTTP API

### `POST /api/records` — submit a document

```bash
curl -F file=@contract.pdf localhost:8080/api/records
```

`202 Accepted`

```json
{
  "id": "8429fcd7-6487-41bf-8281-0be886eb96dd",
  "documentHash": "0x3819dbb7c4c8bf436e8afc20a1deb27e4d16ff6fcf06c45b40f92164abeb5443",
  "status": "PENDING",
  "txHash": null,
  "blockNumber": null,
  "attempts": 0,
  "lastError": null,
  "createdAt": "2026-09-28T20:16:20.073Z",
  "updatedAt": "2026-09-28T20:16:20.073Z"
}
```

`400` if the file is empty. Max upload size is 25 MB.

### `GET /api/records/{id}` — check a submission

Same shape as above; `404` if unknown. Once anchored:

```json
{ "status": "ANCHORED", "txHash": "0x8a88…9977", "blockNumber": 6, "attempts": 1, ... }
```

### `POST /api/verify` — verify a document against the chain

```bash
curl -F file=@contract.pdf localhost:8080/api/verify
```

Anchored — `200 OK`:

```json
{
  "documentHash": "0x3819dbb7c4c8bf436e8afc20a1deb27e4d16ff6fcf06c45b40f92164abeb5443",
  "anchored": true,
  "blockNumber": 6,
  "anchoredAt": "2026-09-28T20:16:21Z",
  "txHash": "0x8a88a3bec75ac68a48760ac5e8b7eb1bb85b767f069069f4115eb5ade4df9977",
  "recordId": "8429fcd7-6487-41bf-8281-0be886eb96dd"
}
```

Not anchored (unknown file, or any byte changed) — also `200 OK`:

```json
{
  "documentHash": "0x8da6ac08a3c8cd58fe49a412c48f0ab64796c8f4bb1ee65f78945df6bbf1c36a",
  "anchored": false,
  "blockNumber": null,
  "anchoredAt": null,
  "txHash": null,
  "recordId": null
}
```

- `anchoredAt` is the anchoring block's timestamp.
- `txHash` can be `null` on an anchored result if the node can't serve the
  event log.
- `recordId` can be set while `anchored` is `false` — a submission that
  hasn't been anchored yet.
- `400` for an empty file; `503` if the chain can't be reached.

---

## Kafka topics

| Topic | Producer | Consumer | Key | Value |
|---|---|---|---|---|
| `records.submitted` | `OutboxRelay` | `AnchoringConsumer` (group `hash-anchor-anchoring`) | record id | outbox payload JSON |
| `records.submitted.DLT` | Spring Kafka `DeadLetterPublishingRecoverer` | — (for inspection / manual re-drive) | record id | original payload, plus `kafka_dlt-*` headers |

Both are created at startup if missing (1 partition each in dev). The
consumer commits offsets per record (`ack-mode: record`) and starts from the
earliest offset when the group has none.

---

## Configuration

`backend/src/main/resources/application.yml`, overridable via environment
variables:

| Property | Env var | Default | |
|---|---|---|---|
| `hashanchor.blockchain.rpc-url` | `HASHANCHOR_RPC_URL` | `http://localhost:8545` | JSON-RPC endpoint |
| `hashanchor.blockchain.private-key` | `HASHANCHOR_PRIVATE_KEY` | **none — required** | Signer; must be the contract owner |
| `hashanchor.blockchain.contract-address` | `HASHANCHOR_CONTRACT_ADDRESS` | **none — required** | Deployed `HashAnchor` |
| `hashanchor.blockchain.receipt-poll-interval` | | `500ms` | |
| `hashanchor.blockchain.receipt-timeout` | | `30s` | Must stay well under Kafka's 5-minute `max.poll.interval.ms` |
| `hashanchor.outbox.poll-interval` | | `5s` | How often `OutboxRelay` publishes pending events |
| `hashanchor.anchoring.stale-after` | `HASHANCHOR_STALE_AFTER` | `5m` | When an `ANCHORING` record counts as stuck |
| `hashanchor.anchoring.stale-check-interval` | | `60s` | |
| `spring.datasource.*` | | `localhost:5432/hashanchor` | Matches `docker-compose.yml` |
| `spring.kafka.bootstrap-servers` | | `localhost:9092` | Matches `docker-compose.yml` |

The private key and contract address have no defaults on purpose: a missing
value fails startup rather than falling back to a placeholder. **Secrets are
never committed** — pass them as environment variables (`.env` files are
gitignored).

---

## Running locally

Prerequisites: Docker, Java 21+, Node.js (for Hardhat).

**1. Start Postgres and Kafka**

```bash
docker compose up -d
```

**2. Start a local chain and deploy the contract** (in `contracts/`)

```bash
cd contracts
npm install
npx hardhat node                  # leave running; prints 20 funded dev accounts
```

In another terminal:

```bash
cd contracts
npx hardhat ignition deploy ignition/modules/HashAnchor.ts --network localhost
# → HashAnchorModule#HashAnchor - 0x5FbDB2315678afecb367f032d93F642f64180aa3
```

(After restarting the Hardhat node, add `--reset`: the new chain is empty and
Ignition's saved deployment state is stale.)

**3. Run the backend** (in `backend/`)

```bash
export HASHANCHOR_PRIVATE_KEY=<Account #0 private key printed by `npx hardhat node`>
export HASHANCHOR_CONTRACT_ADDRESS=<address printed by the deploy>
./gradlew bootRun
```

The build regenerates the web3j contract wrapper from
`contracts/artifacts/`, so run `npx hardhat build` in `contracts/` first if
it complains about a missing artifact.

**4. Try it**

```bash
echo "hello, chain" > doc.txt
curl -F file=@doc.txt localhost:8080/api/records          # → 202, PENDING
curl localhost:8080/api/records/<id>                      # → ANCHORED within a second or two
curl -F file=@doc.txt localhost:8080/api/verify           # → anchored: true
echo "hello, chain!" > doc.txt
curl -F file=@doc.txt localhost:8080/api/verify           # → anchored: false
```

Inspecting the pipeline:

```bash
# records
docker exec hash-anchor-postgres psql -U hashanchor -c "select id, status, attempts, block_number from records"
# dead-lettered events
docker exec hash-anchor-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic records.submitted.DLT --from-beginning --property print.key=true
```

---

## Tests

`./gradlew test` needs nothing: plain unit tests, no Docker, no network.
`./gradlew integrationTest` needs **Docker running**, and nothing else. Those
tests start their own throwaway containers on random ports (Testcontainers)
and never touch the docker compose stack or a hand-started Hardhat node.

**Contracts** (`contracts/`): `npx hardhat test`

**Backend unit tests** (`backend/`): `./gradlew test`

- `AnchoringServiceTest`: the consumer's decision logic with the chain and
  repository mocked. Covers skip on `ANCHORED`/`FAILED`, verify-first,
  resume from `ANCHORING`, failure-then-verify-says-anchored, genuine
  failure, `tx_hash` recovery from logs, and `markFailed`.
- `VerificationServiceTest`: `/api/verify` logic for anchored, unknown,
  chain-overrules-database, database-down, and RPC failure.

**Backend integration tests** (`backend/`): `./gradlew integrationTest`

These run against **Postgres, Kafka and Hardhat node containers**. The
Hardhat image is built from `backend/src/test/resources/hardhat/Dockerfile`
(the same Hardhat version as `contracts/`), and `HashAnchor` is deployed to
it from Java. Tests that boot the app extend `AbstractIntegrationTest`, so
they all share one Spring context, with the outbox relay polling every
200ms instead of 5s.

- `HashAnchorBackendApplicationTests`: smoke test that boots the whole app
  (migrations, JPA validation, Kafka listeners, web server).

- `AnchoringPipelineIntegrationTest`: the full pipeline with nothing mocked.
  Real HTTP to the running app, then outbox, relay, Kafka, consumer, chain,
  and back out through `/api/verify`:
  - a submitted file reaches `ANCHORED` and verifies with the same block,
    `tx_hash` and record id;
  - a file with one bit flipped fails verification (and a never-submitted
    file is reported as not anchored);
  - a duplicate event for an `ANCHORED` record is skipped, with no new
    transaction;
  - a duplicate event after a "lost" DB write is resolved from the chain
    (same block, `tx_hash` from the event log), again with no new
    transaction.
- `AnchorClientIntegrationTest`: `anchor()`, `verify()`, event-log lookup
  and block timestamps against the real chain.

The first `integrationTest` run builds the Hardhat image (about a minute).
It's kept locally as `hash-anchor-test-hardhat:3.18.0` so later runs skip
that; delete it with `docker rmi` if you want the space back. If you bump
Hardhat in `contracts/package.json`, bump `HARDHAT_VERSION` in that
Dockerfile and the image tag in `HardhatChain` to match.

---

## Repository layout

```
hashAnchoringService/
├── docker-compose.yml          Postgres 16 + Kafka 3.8 (KRaft, no ZooKeeper)
├── contracts/                  Hardhat 3 project
│   ├── contracts/HashAnchor.sol
│   ├── test/HashAnchor.ts
│   ├── ignition/modules/HashAnchor.ts
│   └── hardhat.config.ts       networks: localhost (Hardhat node), amoy
└── backend/                    Spring Boot 4 app (Gradle, Kotlin DSL)
    ├── build.gradle.kts        includes web3j wrapper codegen + integrationTest task
    └── src/main/java/com/hashanchor/
        ├── api/                RecordController, VerifyController, response DTOs
        ├── domain/             DocumentRecord, OutboxEvent, RecordStatus,
        │                       RecordService (submit), AnchoringService (consumer logic),
        │                       StaleAnchoringRecovery, VerificationService, DocumentHashes
        ├── persistence/        Spring Data repositories
        ├── messaging/          OutboxRelay, AnchoringConsumer, Topics
        ├── blockchain/         AnchorClient (web3j), BlockchainConfig; wrapper generated into build/
        └── config/             KafkaConsumerConfig (retries, DLT, topics), SchedulingConfig
```

---

## Known tradeoffs

- **A receipt timeout can cause a second `anchor()` transaction.** If no
  receipt arrives within `receipt-timeout`, the attempt fails; `verify()` may
  still say "not anchored" because the first transaction is pending, not
  dropped. The retry then sends a second `anchor()`. Whichever lands second
  reverts and the follow-up `verify()` resolves the record as `ANCHORED` — so
  it's safe, but it wastes gas on the reverted transaction. A fix would be to
  remember the pending transaction's hash and check its receipt before
  re-sending.
- **No confirmation depth yet.** A record is marked `ANCHORED` as soon as the
  receipt arrives (one confirmation), so a chain reorg could undo an anchor
  we've already recorded. Not a concern on the local Hardhat node, which only
  mines a block when a transaction arrives (so waiting for more confirmations
  would hang there). It needs to be added, as a configurable depth, before
  deploying to Polygon Amoy.
- **Fixed gas pricing.** `AnchorClient` uses web3j's `DefaultGasProvider`
  (fixed price and limit) — fine for a local chain; a public network needs a
  fee-aware gas provider.
- **`FAILED` is not re-driven automatically.** Recovering a failed record
  (e.g. after topping up the signer's balance) is a manual operation for now.

---

## Tech stack

- **Backend:** Java 21, Spring Boot 4, Spring Data JPA, Spring Kafka, Flyway,
  Gradle (Kotlin DSL)
- **Data:** PostgreSQL 16, Apache Kafka 3.8 in KRaft mode
- **Chain:** Solidity 0.8.x, Hardhat 3 (local node for development), web3j;
  Polygon Amoy testnet as the eventual deployment target
- **Tests:** JUnit 5, Mockito, AssertJ, Awaitility, Testcontainers (Postgres, Kafka, Hardhat), Hardhat (mocha + ethers)
- **Local infra:** Docker Compose
