package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.MongoNamespace;

final class MapReduceResponseParser {
    private MapReduceResponseParser() {}

    static MongoNamespace getResultsNamespaceFromResponse(final Document response, final String defaultDatabaseName) {
        Object result = response.get("result");
        if (result instanceof Document) {
            Document document = (Document) result;
            String databaseName = document.getString("db") != null ? document.getString("db") : defaultDatabaseName;
            return new MongoNamespace(databaseName, document.getString("collection"));
        } else if (result instanceof String) {
            return new MongoNamespace(defaultDatabaseName, (String) result);
        }
        return null;
    }
}
