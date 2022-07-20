package com.mongodb.client.http;

import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class MongoDatabase {
    private String dbname;
    private Map <String, MongoCollection<Document>> collections;

    private String hostURL;
    public MongoDatabase(String dbname, String hostURL) {
        this.dbname = dbname;
        this.collections = new HashMap<String, MongoCollection<Document>>();
        this.hostURL = hostURL;
    }

    public MongoCollection getCollection(String collectionName) {
        if (!collections.containsKey(collectionName)) {
            collections.put(collectionName, new MongoCollection<Document>(collectionName, dbname, hostURL));
        }
        return collections.get(collectionName);
    }


}
