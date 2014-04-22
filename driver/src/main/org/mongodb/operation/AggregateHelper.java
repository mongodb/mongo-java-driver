package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class AggregateHelper {

    static Document asCommandDocument(final MongoNamespace namespace, final List<Document> pipeline, final AggregationOptions options) {
        Document commandDocument = new Document("aggregate", namespace.getCollectionName());
        commandDocument.put("pipeline", pipeline);
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }
        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            Document cursor = new Document();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", options.getBatchSize());
            }
            commandDocument.put("cursor", cursor);
        }
        if (options.getAllowDiskUse() != null) {
            commandDocument.put("allowDiskUse", options.getAllowDiskUse());
        }
        return commandDocument;
    }

    private AggregateHelper() {
    }
}
