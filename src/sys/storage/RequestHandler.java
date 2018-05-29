package sys.storage;

interface RequestHandler<T> {
	T execute();
}
