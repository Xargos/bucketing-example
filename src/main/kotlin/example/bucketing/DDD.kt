package example.bucketing

import io.vertx.core.Future

interface Aggregate<AGGREGATE_ID>

interface AggregateFactory<AGGREGATE : Aggregate<AGGREGATE_ID>, AGGREGATE_ID>

interface Repository<AGGREGATE : Aggregate<AGGREGATE_ID>, AGGREGATE_ID> {
    fun get(): Future<Collection<AGGREGATE>>
    fun get(id: AGGREGATE_ID): Future<AGGREGATE?>
    fun add(aggregate: AGGREGATE): Future<Void>
    fun remove(id: AGGREGATE_ID): Future<AGGREGATE?>
    fun nextId(): Future<AGGREGATE_ID>
}