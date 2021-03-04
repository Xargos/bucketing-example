package example.bucketing

import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpMethod
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler

data class NewBookDto(private val text: String) : NewBook {
    override fun getText(): String {
        return text
    }
}

class BookDto(val id: BookId, val text: String) {
    constructor(book: Book) : this(book.getId(), book.getText())
}

class HttpVerticle(
    commandExecutor: CommandExecutor,
    private val objectMapper: ObjectMapper,
    bookRepository: BookRepository,
    bookFactory: BookFactory,
    private val port: Int
) : AbstractVerticle() {

    private val bookService = BookService(commandExecutor, bookRepository, bookFactory)

    override fun start(startPromise: Promise<Void>) {

        startHttpServer()
            .onSuccess { startPromise.complete() }
            .onFailure { startPromise.fail(it) }
    }

    private fun startHttpServer(): Future<Void> {
        val server = vertx.createHttpServer()

        val router = Router.router(vertx)

        router.route().handler(BodyHandler.create())

        router.route(HttpMethod.POST, "/book").handler { addBook(it) }
        router.route(HttpMethod.DELETE, "/book/:id").handler { removeBook(it) }
        router.route(HttpMethod.GET, "/book/:id").handler { getBook(it) }
        router.route(HttpMethod.GET, "/book/").handler { getAllBooks(it) }
        val serverStart = Promise.promise<Void>()
        server
            .requestHandler(router)
            .listen(port)
        serverStart.complete()
        return serverStart.future()
    }

    private fun getBook(context: RoutingContext) {
        val bookId = BookId(context.pathParam("id").toInt())
        this.bookService.getBook(bookId)
            .map { it?.let { BookDto(it) } }
            .onSuccess { if (it == null) sendMissingResponse(context) else sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun getAllBooks(context: RoutingContext) {
        this.bookService.getAllBooks()
            .map { it.map { book -> BookDto(book) } }
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun addBook(context: RoutingContext) {
        val newBook = objectMapper.readValue(context.body.toString(), NewBookDto::class.java)
        this.bookService.addBook(newBook)
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun removeBook(context: RoutingContext) {
        val bookId = BookId(context.pathParam("id").toInt())
        this.bookService.removeBook(bookId)
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun <R> sendResponse(routingContext: RoutingContext, it: R) {
        val response = routingContext.response()
        response.putHeader("content-type", "application/json")
        response.end(objectMapper.writeValueAsString(it))
    }

    private fun sendFailedResponse(routingContext: RoutingContext, cause: Throwable) {
        val response = routingContext.response()
        response.statusCode = 500
        response.end(cause.message)
    }

    private fun sendMissingResponse(routingContext: RoutingContext) {
        val response = routingContext.response()
        response.statusCode = 404
        response.end()
    }
}