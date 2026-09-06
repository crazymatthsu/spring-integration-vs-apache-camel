# Kafka manual acknowledgment and batch sizing: notes for future reference

Findings from building the demos, verified against the framework sources shipped in the versions below.
Read the [README](../README.md) first for the overall design.

| Component | Version checked |
|---|---|
| Spring Kafka | 4.1.1 (`KafkaMessageListenerContainer`, `ContainerProperties`) |
| Spring Integration | 7.1.1 |
| Apache Camel | 4.22.0 (`camel-kafka`: `CommitManagers`, `AsyncCommitManager`, `DefaultKafkaManualAsyncCommit`) |
| kafka-clients | 4.2.1 (Spring), 4.3.1 (Camel) |

## 1. Acknowledging from a thread other than the consumer thread

A `KafkaConsumer` is single-threaded by contract: any call from a second thread while the consumer thread is
inside `poll()` fails with `ConcurrentModificationException`. Both use cases acknowledge after asynchronous work,
so the framework has to bridge that gap.

### Spring Kafka: supported

`ContainerProperties.setAckMode(MANUAL)` plus `setAsyncAcks(true)`.

- `Acknowledgment.acknowledge()` may be called from any thread. `processAck` checks the current thread; from a
  non-consumer thread the record is put on an internal queue that the consumer thread drains on its next loop.
  This queue exists with or without `asyncAcks`.
- With `asyncAcks=true` the container keeps the set of offsets delivered by the last poll (`offsetsInThisBatch`)
  and commits a partition only up to its first unacknowledged offset. Out-of-order acknowledgment can never commit
  past a record still in flight.
- While that set is not empty the container pauses the consumer ("Pausing for incomplete async acks") and resumes
  when the last outstanding record is acknowledged. This is natural back-pressure.
- `nack()` is not supported together with `asyncAcks`.

The demos wrap this in `support/spring-kafka-support` (`ManualAckContainers`, about 60 lines).

### Apache Camel 4.22: not supported, built in this repo

- The Kafka component documentation, manual-commit section, "Note 1": records from a partition must be processed
  and committed by the same thread as the consumer; async or concurrent EIPs cause the commit to fail with
  `ConcurrentModificationException`, and the advice is to redesign the route.
- `DefaultKafkaManualAsyncCommitFactory` still exists and the documentation still says the commit "will be done in
  the next consumer loop". In the 4.22.0 sources `DefaultKafkaManualAsyncCommit.commit()` calls
  `AsyncCommitManager.commit(partition)`, which calls `consumer.commitAsync(...)` directly on the calling thread.
  The queue-and-drain design of the 3.x line is gone. `KafkaFetchRecords.startPolling()` has no hook that
  drains parked commits.
- With `allowManualCommit=true` and any factory other than the two default ones, `CommitManagers` selects the
  `NoopCommitManager`, so the framework commits nothing on its own. That is what makes a custom factory safe.
- Camel's supported alternative is the batching consumer (`batching=true`, `maxPollRecords`, `pollTimeoutMs`,
  `batchingIntervalMs`): one exchange per batch, committed after the batch exchange completes. Processing then
  stays on the consumer thread, so it does not give the asynchronous hand-off the use cases ask for.

`support/camel-kafka-support` (about 330 lines) reproduces Spring Kafka's semantics through two public extension
points of the component:

- `DeferredCommitKafkaClientFactory` (`camel.component.kafka.kafka-client-factory`) wraps every consumer in the
  `DeferredCommitConsumer` proxy. The proxy records the offsets returned by each `poll()` and, before every `poll()`,
  `unsubscribe()` and `close()` (all consumer-thread calls), commits each partition up to its first unacknowledged
  record, never moving a position backwards.
- `DeferredKafkaManualCommitFactory` (`camel.component.kafka.kafka-manual-commit-factory`) hands routes a
  `KafkaManualCommit` whose `commit()` only marks the record as acknowledged in a concurrent set.

The first version committed "the highest acknowledged offset per partition". The integration test caught the
consequence within minutes: a poison record acknowledged on the consumer thread was committed ahead of a batch still
in flight, then a later commit moved the offset backwards. Only the contiguous-prefix rule is correct.

State for revoked partitions is dropped after the next poll; rebalance edge cases beyond that are not handled, so
treat the module as demo-grade. Commit latency is bounded by `pollTimeoutMs` (250 ms in the demos).

### Verdict

For "acknowledge from another thread, in any order, at-least-once", Spring Kafka is the mature choice. This is one
capability, not a verdict on the frameworks: in the same demos Camel's `onException(...).handled(true)` and the
aggregate EIP were the simpler constructs.

## 2. What `asyncAcks` implies for batch size

Because the consumer is paused until every record of the last poll is acknowledged, an aggregator downstream never
sees records from two polls at once. Consequences:

- A batch is at most `max.poll.records`. In `usecase1/spring-integration` the property `fixflow.orders.batch-size`
  feeds both the aggregator's release size and `spring.kafka.consumer.max-poll-records` for exactly this reason.
- A poll that returns fewer records than the batch size always waits for `fixflow.orders.batch-timeout` before the
  partial batch is inserted, because the paused consumer cannot top the group up.
- A poison record must still be acknowledged (after logging and skipping it), otherwise its partition stays paused
  forever. Both demos do this.
- Processing one poll has to fit inside `max.poll.interval.ms` (5 minutes by default).

## 3. Plain `AckMode.MANUAL` without `asyncAcks`

Setting `asyncAcks(false)` is the only way to build batches bigger than one poll: the consumer keeps polling while
earlier records are unacknowledged. What changes:

- Cross-thread acknowledgment still works (same queue), but the container now tracks only the highest acknowledged
  offset per partition (`addOffset` uses `Math.max`). Acknowledging record 50 commits 51 even if record 30 is still
  being inserted. A crash at that moment loses record 30: at-most-once instead of at-least-once.
- In `usecase1/spring-integration` that risk is real as written, because batches are released on two threads: the
  `uc1-batch-` executor on size and the scheduler thread on timeout. Batch N+1 can be acknowledged before batch N's
  insert finishes.
- To do it safely: add a single-thread `ExecutorChannel` between the aggregator and the insert so batches are
  inserted and acknowledged strictly in offset order per partition, and accept that this ordering discipline is now
  application code, not framework behaviour.

## 4. Getting bigger JDBC batches in `usecase1/spring-integration`

Configuration only (`usecase1/spring-integration/src/main/resources/application.yml`):

```yaml
fixflow:
  orders:
    batch-size: 500        # aggregator release size, also feeds spring.kafka.consumer.max-poll-records
    batch-timeout: 1s      # a poll smaller than batch-size waits this long before a partial insert
spring:
  kafka:
    consumer:
      properties:
        fetch.min.bytes: 65536     # let the broker fill polls instead of returning a handful of records
        fetch.max.wait.ms: 200
```

Code changes worth making at larger sizes (`OrdersFlowConfiguration`):

1. Wrap the insert in a transaction: `.handle(ordersBatchInsert, e -> e.transactional())`. The JDBC handler runs
   the batch in autocommit mode, so SQLite fsyncs every row; one transaction per batch removes that. Spring Boot
   provides the `DataSourceTransactionManager`.
2. Consider `ListenerMode.batch` on the Kafka inbound adapter: one message per poll with a `List` payload and one
   acknowledgment, no aggregator needed, batch size equals poll size by construction. The trade-off is that the
   batch timeout disappears, so a slow topic yields small batches.

Batches larger than a poll require section 3.

## 5. The same knobs on the Camel side

- `maxPollRecords` on the endpoint bounds a poll; `completionSize` / `completionTimeout` on the aggregate EIP bound
  the batch; `completionTimeoutCheckerInterval` (1 s by default, 100 ms in the demo) bounds how late a timeout fires.
- The deferred-commit proxy does not pause the consumer, so a Camel batch can span polls. The contiguous-prefix rule
  keeps that safe; the cost is more outstanding offsets in memory.
- `pollTimeoutMs` is the commit flush cadence of the proxy when the topic is idle.
- The `sql` endpoint with `batch=true` issues one `executeBatch()`; wrap it with `.transacted()` and a Spring
  transaction manager for one transaction per batch.

## 6. Decision summary

| Requirement | Spring Kafka | Camel 4.22 |
|---|---|---|
| Acknowledge from a worker thread | built in (`asyncAcks`) | custom extension needed (`camel-kafka-support`) |
| Out-of-order acknowledgment stays at-least-once | built in | provided by the custom extension |
| Batch bigger than one poll | only with `asyncAcks=false` and in-order acks in application code | yes, with the custom extension |
| Back-pressure while a batch is in flight | consumer paused automatically | `seda` `blockWhenFull`, or none |
| Poison record | ack it after logging | ack it after logging |
