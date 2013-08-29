package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryCursor;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.ReadPreference.primary;
import static org.mongodb.assertions.Assertions.notNull;

public class GetIndexesOperation<T> extends BaseOperation<List<T>> {
    private final Encoder<Document> simpleDocumentEncoder = new DocumentCodec();
    private final MongoNamespace indexesNamespace;
    private final Find queryForCollectionNamespace;
    private final Decoder<T> resultDecoder;

    public GetIndexesOperation(final BufferProvider bufferProvider, final Session session, final MongoNamespace collectionNamespace,
                               final Decoder<T> resultDecoder) {
        super(bufferProvider, session, false);
        this.resultDecoder = notNull("resultDecoder", resultDecoder);
        notNull("collectionNamespace", collectionNamespace);
        this.indexesNamespace = new MongoNamespace(collectionNamespace.getDatabaseName(), "system.indexes");
        this.queryForCollectionNamespace = new Find(new Document("ns", collectionNamespace.getFullName())).readPreference(primary());
    }

    @Override
    public List<T> execute() {
        final List<T> retVal = new ArrayList<T>();
        final MongoCursor<T> cursor = new MongoQueryCursor<T>(indexesNamespace, queryForCollectionNamespace, simpleDocumentEncoder,
                                                              resultDecoder, getBufferProvider(), getSession(), isCloseSession());
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        return retVal;
    }
}
