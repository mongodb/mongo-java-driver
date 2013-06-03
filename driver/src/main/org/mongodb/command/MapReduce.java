package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MapReduceOutput;

public class MapReduce extends Command {

    public MapReduce(final org.mongodb.operation.MapReduce mapReduce, final String collectionName) {
        super(asDocument(mapReduce, collectionName));
    }

    private static Document asDocument(final org.mongodb.operation.MapReduce mapReduce, final String collectionName) {

        return new Document("mapReduce", collectionName)
                .append("map", mapReduce.getMapFunction())
                .append("reduce", mapReduce.getReduceFunction())
                .append("out", mapReduce.isInline() ? new Document("inline", 1) : asDocument(mapReduce.getOutput()))
                .append("query", mapReduce.getFilter())
                .append("sort", mapReduce.getSortCriteria())
                .append("limit", mapReduce.getLimit())
                .append("finalize", mapReduce.getFinalizeFunction())
                .append("scope", mapReduce.getScope())
                .append("jsMode", mapReduce.isJsMode())
                .append("verbose", mapReduce.isVerbose());
    }

    private static Document asDocument(final MapReduceOutput output) {
        final Document document = new Document(output.getAction().getValue(), output.getCollectionName());
        if (output.getDatabaseName() != null) {
            document.append("db", output.getDatabaseName());
        }
        document.append("sharded", output.isSharded());
        document.append("nonAtomic", output.isNonAtomic());

        return document;
    }

}
