package example.bucketing

import io.vertx.core.Future

data class BookId(val id: Int)

data class Book(val text: String)

// Verticle runs on a single thread so we don't have to worry about threadsafety
// Makes things very convenient
data class Bookshelf(val books: MutableMap<BookId, Book>)

data class GetBook(val bookId: BookId) : Query<Bookshelf, Book?> {
    override fun run(aggregate: Bookshelf) = aggregate.books[bookId]
}

object GetAllBooks : Query<Bookshelf, Collection<Book>> {
    override fun run(aggregate: Bookshelf) = aggregate.books.values
}

data class AddBook(val book: Book) : Command<Bookshelf, BookId> {
    override fun run(aggregate: Bookshelf): BookId {
        val bookId = determineBookId(aggregate)
        aggregate.books[bookId] = book
        return bookId
    }

    private fun determineBookId(aggregate: Bookshelf): BookId {
        return if (aggregate.books.isEmpty()) {
            BookId(0)
        } else {
            BookId(aggregate.books.keys.maxOf { it.id } + 1)
        }
    }
}

data class RemoveBook(val bookId: BookId) : Query<Bookshelf, Book?> {
    override fun run(aggregate: Bookshelf) = aggregate.books.remove(bookId)
}

class BookshelfService(
    private val commandExecutor: CommandExecutor,
    private val bookshelf: Bookshelf = Bookshelf(mutableMapOf())
) {
    fun getBook(bookId: BookId): Future<Book?> {
        return commandExecutor.exec(this.bookshelf, GetBook(bookId))
    }

    fun getAllBooks(): Future<Collection<Book>> {
        return commandExecutor.exec(this.bookshelf, GetAllBooks)
    }

    fun addBook(book: Book): Future<BookId> {
        return commandExecutor.exec(this.bookshelf, AddBook(book))
    }

    fun removeBook(bookId: BookId): Future<Book?> {
        return commandExecutor.exec(this.bookshelf, RemoveBook(bookId))
    }
}