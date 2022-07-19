package com.mongodb.client.http;

import org.bson.Document;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.HashMap;
import java.util.Map;

public class MongoDatabase {

    private String dbname;
    private Map <String, MongoCollection<Document>> collections;

    private String hostURL;
    private static final Logger LOGGER = Loggers.getLogger("MongoDatabase");
    public MongoDatabase(String dbname, String hostURL) {
        this.dbname = dbname;
        this.collections = new HashMap<String, MongoCollection<Document>>();
        this.hostURL = hostURL;
    }

    public MongoCollection getCollection(String collectionName) {
        LOGGER.info("http MongoDatabase 19");
        if (!collections.containsKey(collectionName)) {
            collections.put(collectionName, new MongoCollection<Document>(collectionName, dbname, hostURL));
        }
        LOGGER.info("http MongoDatabase 23");
        return collections.get(collectionName);
    }


}
