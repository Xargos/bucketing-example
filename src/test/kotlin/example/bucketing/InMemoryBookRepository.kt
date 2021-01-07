package example.bucketing

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx

class InMemoryBookRepository(
    private val vertx: Vertx,
    private val delay: Long = 1000
) : BookRepository {

    // Verticle runs on a single thread so we don't have to worry about threadsafety
    // Makes things very convenient
    private val store = mutableMapOf<BookId, Book>()
    private var idCounter = 0
    var callNo = 0

    override fun get(): Future<Collection<Book>> {
        callNo++
        val promise = Promise.promise<Collection<Book>>()
        vertx.setTimer(delay) {
            promise.complete(store.values)
        }
        return promise.future()
    }

    override fun get(id: BookId): Future<Book?> {
        callNo++
        val promise = Promise.promise<Book?>()
        vertx.setTimer(delay) {
            promise.complete(store[id])
        }
        return promise.future()
    }

    override fun add(aggregate: Book): Future<Void> {
        callNo++
        val promise = Promise.promise<Void>()
        vertx.setTimer(delay) {
            store[aggregate.bookId] = aggregate
            promise.complete()
        }
        return promise.future()
    }

    override fun remove(id: BookId): Future<Book?> {
        callNo++
        val promise = Promise.promise<Book?>()
        vertx.setTimer(delay) {
            promise.complete(store.remove(id))
        }
        return promise.future()
    }

    override fun nextId(): Future<BookId> {
        callNo++
        val promise = Promise.promise<BookId>()
        vertx.setTimer(delay) {
            promise.complete(BookId(idCounter++))
        }
        return promise.future()
    }
}