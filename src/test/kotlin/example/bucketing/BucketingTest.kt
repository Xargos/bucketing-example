package example.bucketing

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpMethod
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith


@ExtendWith(VertxExtension::class)
class BucketingTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    @Test
    fun verifyBasicFunctionality(vertx: Vertx, testContext: VertxTestContext) {
        val newBook = NewBook("TEST")

        val createHttpClient = vertx.createHttpClient()
        vertx.deployVerticle(HttpVerticle(SimpleCommandExecutor(), objectMapper, InMemoryBookRepository(vertx), 8080))
            .compose { addBook(createHttpClient, newBook) }
            .compose { getBook(createHttpClient, it) }
            .compose { response ->
                response.body()
                    .map { objectMapper.readValue(it.toString(), Book::class.java) }
                    .map { assert(it.text == newBook.text) }
            }
            .onSuccess { testContext.completeNow() }
            .onFailure { testContext.failNow(it) }
    }

    @Test
    fun runWithoutBucketing(vertx: Vertx, testContext: VertxTestContext) {
        val repository = InMemoryBookRepository(vertx)
        val createHttpClient = vertx.createHttpClient()
        vertx.deployVerticle(HttpVerticle(SimpleCommandExecutor(), objectMapper, repository, 8080))
            .compose { populateBookshelf(createHttpClient) }
            .compose { sendQueries(createHttpClient, 100, it) }
            .map { compositeFuture ->
                if (compositeFuture.list<Int>().all { it == 200 }) {
                    println(repository.callNo)
                    assert(repository.callNo == 106)
                } else {
                    testContext.failNow("Something went wrong")
                }
            }
            .onSuccess { testContext.completeNow() }
            .onFailure { testContext.failNow(it) }
    }

    @Test
    fun runWithBucketing(vertx: Vertx, testContext: VertxTestContext) {
        val repository = InMemoryBookRepository(vertx)
        val createHttpClient = vertx.createHttpClient()
        vertx.deployVerticle(HttpVerticle(BucketingCommandExecutor(), objectMapper, repository, 8080))
            .compose { populateBookshelf(createHttpClient) }
            .compose { sendQueries(createHttpClient, 100, it) }
            .map { compositeFuture ->
                if (compositeFuture.list<Int>().all { it == 200 }) {
                    println(repository.callNo)
                    assert(repository.callNo < 106)
                } else {
                    testContext.failNow("Something went wrong")
                }
            }
            .onSuccess { testContext.completeNow() }
            .onFailure { testContext.failNow(it) }
    }

    private fun populateBookshelf(createHttpClient: HttpClient): Future<List<BookId>> {

        val book1 = NewBook("Some book 1")
        val book2 = NewBook("Some book 2")
        val book3 = NewBook("Some book 3")


        return CompositeFuture.all(
            addBook(createHttpClient, book1),
            addBook(createHttpClient, book2),
            addBook(createHttpClient, book3),
        )
            .map { it.list() }
    }

    private fun sendQueries(createHttpClient: HttpClient, noOfQueries: Int, bookIds: List<BookId>) =
        CompositeFuture.all((1..noOfQueries).map {
            getBook(
                createHttpClient,
                bookIds.random()
            ).map { it.statusCode() }
        })

    private fun getBook(createHttpClient: HttpClient, bookId: BookId) =
        createHttpClient.request(HttpMethod.GET, 8080, "localhost", "/book/${bookId.id}")
            .compose { it.send() }

    private fun addBook(createHttpClient: HttpClient, newBook: NewBook): Future<BookId> =
        createHttpClient.request(HttpMethod.POST, 8080, "localhost", "/book")
            .compose { it.send(objectMapper.writeValueAsString(newBook)) }
            .compose { it.body() }
            .map { objectMapper.readValue(it.toString(), BookId::class.java) }
}