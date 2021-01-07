package example.bucketing

import io.vertx.core.Future
import io.vertx.core.Promise

interface Operation<AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> {
    fun run(repository: REPOSITORY): Future<RESULT>
}

interface Command<AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> :
    Operation<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>

interface Query<AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> :
    Operation<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>

interface CommandExecutor {
    fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT>

    fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT>
}

class SimpleCommandExecutor : CommandExecutor {
    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT> {
        return command.run(repository)
    }

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT> {
        return query.run(repository)
    }
}

class BucketingCommandExecutor : CommandExecutor {
    private val bucket =
        mutableMapOf<Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>, MutableSet<Promise<Any>>>()

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT> {
        return command.run(repository)
    }

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<RESULT> {
        val result = Promise.promise<RESULT>()
        return if (bucket.containsKey(query)) {
            bucket[query]?.add(result as Promise<Any>)
            result.future()
        } else {
            val results = mutableSetOf(result as Promise<Any>)
            bucket[query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>] = results
            query.run(repository)
                .onSuccess { res ->
                    bucket.remove(query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>)
                    results.forEach { it.complete(res) }
                }
                .onFailure { res ->
                    bucket.remove(query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>)
                    results.forEach { it.fail(res) }
                }
            result.future() as Future<RESULT>
        }
    }
}