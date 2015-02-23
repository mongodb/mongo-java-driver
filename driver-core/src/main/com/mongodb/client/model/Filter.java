package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A representation of a query filter.
 *
 * @since 3.0
 */
public abstract class Filter {

    /**
     * Render the filter into a BsonDocument.
     *
     * @param documentClass the document class in scope for the collection
     * @param codecRegistry the codec registry
     * @param <TDocument> the type of the document class
     * @return the filter as a BsonDocument
     */
    public abstract <TDocument> BsonDocument render(Class<TDocument> documentClass, CodecRegistry codecRegistry);

    /**
     * A static factory method that converts a BsonDocument to a Filter.
     *
     * @param document the document
     * @return the filter
     */
    public static Filter asFilter(final BsonDocument document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> documentClass, final CodecRegistry codecRegistry) {
                return document;
            }
        };
    }

    /**
     * A static factory method that converts a Document to a Filter.
     *
     * @param document the document
     * @return the filter
     */
    public static Filter asFilter(final Document document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> documentClass, final CodecRegistry codecRegistry) {
                return new BsonDocumentWrapper<Document>(document, codecRegistry.get(Document.class));
            }
        };
    }

    /**
     * A static factory method that converts a JSON string to a Filter.
     *
     * @param json the document as a JSON string
     * @return the filter
     */
    public static Filter asFilter(final String json) {
        return new Filter() {
            @Override
            public <T> BsonDocument render(final Class<T> documentClass, final CodecRegistry codecRegistry) {
                return BsonDocument.parse(json);
            }
        };
    }

    /**
     * A static factory method that converts any Object to a Filter.  Use this with care.
     *
     * @param document the document
     * @return the filter
     */
    public static Filter asFilter(final Object document) {
        return new Filter() {
            @Override
            public <C> BsonDocument render(final Class<C> documentClass, final CodecRegistry codecRegistry) {
                if (document instanceof String) {
                    return asFilter((String) document).render(documentClass, codecRegistry);
                }
                return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
            }
        };
    }
}


