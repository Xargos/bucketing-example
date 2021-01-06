package example.bucketing

import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpMethod
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler

data class HttpConfig(val commandExecutor: CommandExecutor, val port: Int, val objectMapper: ObjectMapper)

fun buildHttpVerticle(
    commandExecutor: CommandExecutor,
    objectMapper: ObjectMapper
): HttpVerticle {
    val httpConfig = HttpConfig(commandExecutor, 8080, objectMapper)
    return HttpVerticle(
        config = httpConfig
    )
}

class HttpVerticle(
    private val config: HttpConfig
) : AbstractVerticle() {

    private val bookshelfService = BookshelfService(this.config.commandExecutor)

    override fun start(startPromise: Promise<Void>) {

        startHttpServer()
            .onSuccess { startPromise.complete() }
            .onFailure { startPromise.fail(it) }
    }

    private fun startHttpServer(): Future<Void> {
        val server = vertx.createHttpServer()

        val router = Router.router(vertx)

        router.route().handler(BodyHandler.create())

        router.route(HttpMethod.POST, "/bookshelf").handler { addBook(it) }
        router.route(HttpMethod.DELETE, "/bookshelf/:id").handler { removeBook(it) }
        router.route(HttpMethod.GET, "/bookshelf/:id").handler { getBook(it) }
        router.route(HttpMethod.GET, "/bookshelf/").handler { getAllBooks(it) }
        val serverStart = Promise.promise<Void>()
        server
            .requestHandler(router)
            .listen(config.port)
        serverStart.complete()
        return serverStart.future()
    }

    private fun getBook(context: RoutingContext) {
        val bookId = BookId(context.pathParam("id").toInt())
        this.bookshelfService.getBook(bookId)
            .onSuccess { if (it == null) sendMissingResponse(context) else sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun getAllBooks(context: RoutingContext) {
        this.bookshelfService.getAllBooks()
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun addBook(context: RoutingContext) {
        val book = this.config.objectMapper.readValue(context.body.toString(), Book::class.java)
        this.bookshelfService.addBook(book)
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun removeBook(context: RoutingContext) {
        val bookId = BookId(context.pathParam("id").toInt())
        this.bookshelfService.removeBook(bookId)
            .onSuccess { sendResponse(context, it) }
            .onFailure { sendFailedResponse(context, it) }
    }

    private fun <R> sendResponse(routingContext: RoutingContext, it: R) {
        val response = routingContext.response()
        response.putHeader("content-type", "application/json")
        response.end(this.config.objectMapper.writeValueAsString(it))
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