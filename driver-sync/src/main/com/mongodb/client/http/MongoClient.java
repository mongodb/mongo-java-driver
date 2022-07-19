package com.mongodb.client.http;

import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.HashMap;
import java.util.Map;

public class MongoClient {

    private Map<String, MongoDatabase> databases;
    private static final Logger LOGGER = Loggers.getLogger("MongoClient");
    public MongoClient () {
        this.databases = new HashMap<String, MongoDatabase>();
    }

    public MongoDatabase getDatabase(String databaseName) {
        LOGGER.info("http Mongoclient 17");
        if (databases.containsKey(databaseName)) {
            return databases.get(databaseName);
        } else {
            databases.put(databaseName, new MongoDatabase(databaseName));
            return databases.get(databaseName);
        }
    }
}
