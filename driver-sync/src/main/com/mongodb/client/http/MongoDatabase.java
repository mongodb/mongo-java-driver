package com.mongodb.client.http;

import org.bson.Document;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.HashMap;
import java.util.Map;

public class MongoDatabase {

    private String dbname;
    private Map <String, MongoCollection<Document>> collections;

    private static final Logger LOGGER = Loggers.getLogger("MongoDatabase");
    public MongoDatabase(String dbname) {
        this.dbname = dbname;
        this.collections = new HashMap<String, MongoCollection<Document>>();
    }

    public MongoCollection getCollection(String collectionName) {
        LOGGER.info("http MongoDatabase 19");
        if (!collections.containsKey(collectionName)) {
            collections.put(collectionName, new MongoCollection<Document>(collectionName, dbname));
        }
        LOGGER.info("http MongoDatabase 23");
        return collections.get(collectionName);
    }


}
