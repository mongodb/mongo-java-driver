package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.MongoNamespace;

final class MapReduceResponseParser {
    private MapReduceResponseParser() {}

    static MongoNamespace getResultsNamespaceFromResponse(final Document response, final String defaultDatabaseName) {
        Object result = response.get("result");
        if (result instanceof Document) {
            return getNamespaceFromResultDocument(defaultDatabaseName, (Document) result);
        } else if (result instanceof String) {
            return new MongoNamespace(defaultDatabaseName, (String) result);
        }
        return null;
    }

    private static MongoNamespace getNamespaceFromResultDocument(final String defaultDatabaseName, final Document document) {
        String databaseNameFromDocument = document.getString("db");
        String databaseName = databaseNameFromDocument != null ? databaseNameFromDocument : defaultDatabaseName;
        return new MongoNamespace(databaseName, document.getString("collection"));
    }
}
