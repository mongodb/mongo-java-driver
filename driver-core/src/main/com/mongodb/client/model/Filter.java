package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

public abstract class Filter {

    public abstract <TDocument> BsonDocument render(Class<TDocument> collectionClass, CodecRegistry codecRegistry);

    public static Filter asFilter(final BsonDocument document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> collectionClass, final CodecRegistry codecRegistry) {
                return document;
            }
        };
    }

    public static Filter asFilter(final Document document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> collectionClass, final CodecRegistry codecRegistry) {
                return new BsonDocumentWrapper<Document>(document, codecRegistry.get(Document.class));
            }
        };
    }

    public static Filter asFilter(final String json) {
        return new Filter() {
            @Override
            public <T> BsonDocument render(final Class<T> collectionClass, final CodecRegistry codecRegistry) {
                return BsonDocument.parse(json);
            }
        };
    }

    public static Filter asFilter(final Object document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> collectionClass, final CodecRegistry codecRegistry) {
                if (document instanceof String) {
                    return asFilter((String) document).render(collectionClass, codecRegistry);
                }
                return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
            }
        };
    }
}


