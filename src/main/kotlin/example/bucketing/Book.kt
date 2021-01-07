package example.bucketing

import io.vertx.core.Future

data class BookId(val id: Int)

data class NewBook(val text: String)

data class Book(val bookId: BookId, val text: String) : Aggregate<BookId>

interface BookRepository : Repository<Book, BookId>

data class GetBook(val bookId: BookId) : Query<BookId, Book, BookRepository, Book?> {
    override fun run(repository: BookRepository) = repository.get(bookId)
}

object GetAllBooks : Query<BookId, Book, BookRepository, Collection<Book>> {
    override fun run(repository: BookRepository) = repository.get()
}

data class AddBook(val newBook: NewBook) : Command<BookId, Book, BookRepository, BookId> {
    override fun run(repository: BookRepository): Future<BookId> =
        repository.nextId()
            .compose { bookId -> repository.add(Book(bookId, newBook.text)).map { bookId } }
}

data class RemoveBook(val bookId: BookId) : Query<BookId, Book, BookRepository, Book?> {
    override fun run(repository: BookRepository): Future<Book?> = repository.remove(bookId)
}

class BookService(
    private val commandExecutor: CommandExecutor,
    private val bookRepository: BookRepository
) {
    fun getBook(bookId: BookId): Future<Book?> {
        return commandExecutor.exec(this.bookRepository, GetBook(bookId))
    }

    fun getAllBooks(): Future<Collection<Book>> {
        return commandExecutor.exec(this.bookRepository, GetAllBooks)
    }

    fun addBook(newBook: NewBook): Future<BookId> {
        return commandExecutor.exec(this.bookRepository, AddBook(newBook))
    }

    fun removeBook(bookId: BookId): Future<Book?> {
        return commandExecutor.exec(this.bookRepository, RemoveBook(bookId))
    }
}