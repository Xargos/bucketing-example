package example.bucketing

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx

interface Operation<AGGREGATE, RESULT> {
    fun run(aggregate: AGGREGATE): RESULT
}

interface Command<AGGREGATE, RESULT> : Operation<AGGREGATE, RESULT>

interface Query<AGGREGATE, RESULT> : Operation<AGGREGATE, RESULT>

interface CommandExecutor {
    fun <AGGREGATE, RESULT> exec(aggregate: AGGREGATE, operation: Operation<AGGREGATE, RESULT>): Future<RESULT>
}

class SimpleCommandExecutor(private val vertx: Vertx) : CommandExecutor {
    override fun <AGGREGATE, RESULT> exec(
        aggregate: AGGREGATE,
        operation: Operation<AGGREGATE, RESULT>
    ): Future<RESULT> {
        val promise = Promise.promise<RESULT>()
        vertx.setTimer(1000) {
            promise.complete(operation.run(aggregate))
        }
        return promise.future()
    }
}