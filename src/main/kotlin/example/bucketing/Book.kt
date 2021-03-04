package example.bucketing

import io.vertx.core.Future

data class BookId(val id: Int)

interface NewBook {
    fun getText(): String
}

interface Book : Aggregate<BookId> {
    fun getId(): BookId
    fun getText(): String
}

interface BookFactory : AggregateFactory<Book, BookId> {
    fun build(id: BookId, text: String): Book
}

interface BookRepository : Repository<Book, BookId>

data class GetBook(val bookId: BookId) : Query<BookId, Book, BookRepository, Book?> {
    override fun run(repository: BookRepository) = repository.get(bookId)
}

object GetAllBooks : Query<BookId, Book, BookRepository, Collection<Book>> {
    override fun run(repository: BookRepository) = repository.get()
}

data class AddBook(val newBook: NewBook, val bookFactory: BookFactory) : Command<BookId, Book, BookRepository, BookId> {
    override fun run(repository: BookRepository): Future<BookId> =
        repository.nextId()
            .compose { bookId -> repository.add(bookFactory.build(bookId, newBook.getText())).map { bookId } }
}

data class RemoveBook(val bookId: BookId) : Command<BookId, Book, BookRepository, Book?> {
    override fun run(repository: BookRepository): Future<Book?> = repository.remove(bookId)
}

class BookService(
    private val commandExecutor: CommandExecutor,
    private val bookRepository: BookRepository,
    private val bookFactory: BookFactory
) {
    fun getBook(bookId: BookId): Future<Book?> {
        return this.execQuery(GetBook(bookId))
    }

    fun getAllBooks(): Future<Collection<Book>> {
        return this.execQuery(GetAllBooks)
    }

    fun addBook(newBook: NewBook): Future<BookId> {
        return this.execCommand(AddBook(newBook, bookFactory))
    }

    fun removeBook(bookId: BookId): Future<Book?> {
        return this.execCommand(RemoveBook(bookId))
    }

    private fun <RESULT> execCommand(operation: Command<BookId, Book, BookRepository, RESULT>): Future<RESULT> {
        return commandExecutor.exec(this.bookRepository, operation)
    }

    private fun <RESULT> execQuery(operation: Query<BookId, Book, BookRepository, RESULT>): Future<RESULT> {
        return commandExecutor.exec(this.bookRepository, operation)
    }
}