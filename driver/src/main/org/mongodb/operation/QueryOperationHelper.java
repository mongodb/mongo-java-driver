package org.mongodb.operation;

import org.mongodb.AsyncBlock;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Function;
import org.mongodb.MongoCursor;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ConnectionSource;
import org.mongodb.binding.ReadBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static org.mongodb.operation.OperationHelper.IdentityTransformer;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.withConnection;

final class QueryOperationHelper {
    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                         final ReadBinding binding) {
        return queryResultToList(namespace, queryProtocol, decoder, binding, new Function<T, T>() {
            @Override
            public T apply(final T t) {
                return t;
            }
        });
    }

    static <V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                         final ReadBinding binding, final Function<Document, V> block) {
        return queryResultToList(namespace, queryProtocol, new DocumentCodec(), binding, block);
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                            final ReadBinding binding, final Function<T, V> block) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return queryResultToList(namespace, executeProtocol(queryProtocol, source), decoder, source, block);
        } finally {
            source.release();
        }
    }

    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                         final ConnectionSource source) {
        return queryResultToList(namespace, queryResult, decoder, source, new IdentityTransformer<T>());
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                            final ConnectionSource source,
                                            final Function<T, V> block) {
        MongoCursor<T> cursor = new MongoQueryCursor<T>(namespace, queryResult, 0, 0, decoder, source);
        try {
            List<V> retVal = new ArrayList<V>();
            while (cursor.hasNext()) {
                V value = block.apply(cursor.next());
                if (value != null) {
                    retVal.add(value);
                }
            }
            return unmodifiableList(retVal);
        } finally {
            cursor.close();
        }
    }

    static MongoFuture<List<Document>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                                              final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, binding, new IdentityTransformer<Document>());
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                                           final AsyncReadBinding binding, final Function<Document, T> transformer) {
        return queryResultToListAsync(namespace, queryProtocol, new DocumentCodec(), binding, transformer);
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                           final Decoder<T> decoder, final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, decoder, binding, new IdentityTransformer<T>());
    }

    static <T, V> MongoFuture<List<V>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                              final Decoder<T> decoder, final AsyncReadBinding binding,
                                                              final Function<T, V> transformer) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<List<V>>() {
            @Override
            public MongoFuture<List<V>> call(final AsyncConnectionSource source, final Connection connection) {
                final SingleResultFuture<List<V>> future = new SingleResultFuture<List<V>>();
                queryProtocol.executeAsync(connection)
                             .register(new QueryResultToListCallback<T, V>(future, namespace, decoder, source, transformer));
                return future;
            }
        });
    }

    private static class QueryResultToListCallback<T, V> implements SingleResultCallback<QueryResult<T>> {

        private SingleResultFuture<List<V>> future;
        private MongoNamespace namespace;
        private Decoder<T> decoder;
        private AsyncConnectionSource connectionSource;
        private Function<T, V> block;

        public QueryResultToListCallback(final SingleResultFuture<List<V>> future,
                                         final MongoNamespace namespace,
                                         final Decoder<T> decoder,
                                         final AsyncConnectionSource connectionSource,
                                         final Function<T, V> block) {
            this.future = future;
            this.namespace = namespace;
            this.decoder = decoder;
            this.connectionSource = connectionSource;
            this.block = block;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                MongoAsyncQueryCursor<T> cursor = new MongoAsyncQueryCursor<T>(namespace,
                                                                               result,
                                                                               0, 0, decoder,
                                                                               connectionSource);

                final List<V> results = new ArrayList<V>();
                cursor.start(new AsyncBlock<T>() {

                    @Override
                    public void done() {
                        future.init(unmodifiableList(results), null);
                    }

                    @Override
                    public void apply(final T v) {
                        V value = block.apply(v);
                        if (value != null) {
                            results.add(value);
                        }
                    }
                });
            }
        }
    }

    private QueryOperationHelper() {
    }
}
