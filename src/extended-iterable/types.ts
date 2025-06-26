export type IterableLike<T> =
	| Iterator<T>
	| AsyncIterator<T>
	| Iterable<T>
	| (() => Generator<T>)
	| (() => AsyncGenerator<T>);
