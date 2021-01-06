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
    fun runWithoutBucketing(vertx: Vertx, testContext: VertxTestContext) {
        val createHttpClient = vertx.createHttpClient()
        vertx.deployVerticle(buildHttpVerticle(SimpleCommandExecutor(vertx), objectMapper))
            .compose {
                populateBookshelf(createHttpClient)
            }
            .compose {
                sendQueries(createHttpClient, 10, it)
            }
//            .compose {
//                print(it)
//                return@compose it
//            }
            .onSuccess { compositeFuture ->
                if (compositeFuture.list<Int>().all { it == 200 }) {
                    testContext.completeNow()
                } else {
                    testContext.failNow("Something went wrong")
                }
            }
            .onFailure { testContext.failNow(it) }
    }

    private fun populateBookshelf(createHttpClient: HttpClient): Future<List<BookId>> {

        val book1 = Book("Some book 1")
        val book2 = Book("Some book 2")
        val book3 = Book("Some book 3")


        return CompositeFuture.all(
            addBook(createHttpClient, book1),
            addBook(createHttpClient, book2),
            addBook(createHttpClient, book3),
        )
            .map { it.list() }
    }

    private fun sendQueries(createHttpClient: HttpClient, noOfQueries: Int, bookIds: List<BookId>) =
        CompositeFuture.all((1..noOfQueries).map { getBook(createHttpClient, bookIds.random()) })

    private fun getBook(createHttpClient: HttpClient, bookId: BookId) =
        createHttpClient.request(HttpMethod.GET, 8080, "localhost", "/bookshelf/${bookId.id}")
            .compose { it.send() }
            .map { it.statusCode() }

    private fun addBook(createHttpClient: HttpClient, book: Book): Future<BookId> =
        createHttpClient.request(HttpMethod.POST, 8080, "localhost", "/bookshelf")
            .compose { it.send(objectMapper.writeValueAsString(book)) }
            .compose { it.body() }
            .map { objectMapper.readValue(it.toString(), BookId::class.java) }
}