package example.bucketing

import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.function.Function

interface OperationResultHandler<RESULT, HANDLER_RESULT> {
    fun handle(result: RESULT): HANDLER_RESULT
    fun handleException(throwable: Throwable): HANDLER_RESULT
}

fun <RESULT> genericHandler(): OperationResultHandler<RESULT, RESULT> {
    return OperationResultHandlerGeneric()
}

class OperationResultHandlerGeneric<RESULT> : OperationResultHandler<RESULT, RESULT> {
    override fun handle(result: RESULT): RESULT {
        return result
    }

    override fun handleException(throwable: Throwable): RESULT {
        throw throwable
    }

}

interface OperationResult<RESULT, HANDLER_RESULT> {
    fun get(resultHandler: OperationResultHandler<RESULT, HANDLER_RESULT>): HANDLER_RESULT

    fun <MAPPED_RESULT, MAPPED_HANDLER_RESULT> map(f: Function<RESULT, MAPPED_RESULT>): OperationResult<MAPPED_RESULT, MAPPED_HANDLER_RESULT>
}

fun <RESULT, HANDLER_RESULT> operationSuccess(result: RESULT): OperationResult<RESULT, HANDLER_RESULT> {
    return OperationResultSuccess(result)
}

fun <RESULT, HANDLER_RESULT> operationException(throwable: Throwable): OperationResult<RESULT, HANDLER_RESULT> {
    return OperationResultException(throwable)
}

class OperationResultSuccess<RESULT, HANDLER_RESULT>(private val result: RESULT) :
    OperationResult<RESULT, HANDLER_RESULT> {
    override fun get(resultHandler: OperationResultHandler<RESULT, HANDLER_RESULT>): HANDLER_RESULT {
        return resultHandler.handle(result)
    }

    override fun <MAPPED_RESULT, MAPPED_HANDLER_RESULT> map(f: Function<RESULT, MAPPED_RESULT>): OperationResult<MAPPED_RESULT, MAPPED_HANDLER_RESULT> {
        return operationSuccess(f.apply(result))
    }
}

class OperationResultException<RESULT, HANDLER_RESULT>(private val throwable: Throwable) :
    OperationResult<RESULT, HANDLER_RESULT> {
    override fun get(resultHandler: OperationResultHandler<RESULT, HANDLER_RESULT>): HANDLER_RESULT {
        return resultHandler.handleException(throwable)
    }

    override fun <MAPPED_RESULT, MAPPED_HANDLER_RESULT> map(f: Function<RESULT, MAPPED_RESULT>): OperationResult<MAPPED_RESULT, MAPPED_HANDLER_RESULT> {
        return operationException(throwable)
    }
}

interface Operation<
        AGGREGATE_ID,
        AGGREGATE : Aggregate<AGGREGATE_ID>,
        REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>,
        RESULT
        > {
    fun run(repository: REPOSITORY): Future<RESULT>
}

interface Command<AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> :
    Operation<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>

interface Query<AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT> :
    Operation<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>

interface CommandExecutor {
    fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>>

    fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>>
}

class SimpleCommandExecutor : CommandExecutor {
    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>> {
        return command.run(repository)
            .map { operationSuccess<RESULT, HANDLER_RESULT>(it) }
            .otherwise { operationException(it) }
    }

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>> {
        return query.run(repository)
            .map { operationSuccess<RESULT, HANDLER_RESULT>(it) }
            .otherwise { operationException(it) }
    }
}

class BucketingCommandExecutor : CommandExecutor {
    private val bucket =
        mutableMapOf<Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>, MutableSet<Promise<Any>>>()

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        command: Command<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>> {
        return command.run(repository)
            .map { operationSuccess<RESULT, HANDLER_RESULT>(it) }
            .otherwise { operationException(it) }
    }

    override fun <AGGREGATE_ID, AGGREGATE : Aggregate<AGGREGATE_ID>, REPOSITORY : Repository<AGGREGATE, AGGREGATE_ID>, RESULT, HANDLER_RESULT> exec(
        repository: REPOSITORY,
        query: Query<AGGREGATE_ID, AGGREGATE, REPOSITORY, RESULT>
    ): Future<OperationResult<RESULT, HANDLER_RESULT>> {
        val result = Promise.promise<OperationResult<RESULT, HANDLER_RESULT>>()
        return if (bucket.containsKey(query)) {
            bucket[query]?.add(result as Promise<Any>)
            result.future()
        } else {
            val results = mutableSetOf(result as Promise<Any>)
            bucket[query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>] = results
            query.run(repository)
                .onSuccess { res ->
                    bucket.remove(query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>)
                    results.forEach { it.complete(operationSuccess<RESULT, Any>(res)) }
                }
                .onFailure { res ->
                    bucket.remove(query as Query<Any, Aggregate<Any>, Repository<Aggregate<Any>, Any>, Any>)
                    results.forEach { it.complete(operationException<RESULT, Any>(res)) }
                }
            result.future() as Future<OperationResult<RESULT, HANDLER_RESULT>>
        }
    }
}