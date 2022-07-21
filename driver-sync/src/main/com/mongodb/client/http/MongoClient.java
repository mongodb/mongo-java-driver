package com.mongodb.client.http;

import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

public class MongoClient {

    private Map<String, MongoDatabase> databases;
    private final String hostURL;

    private CodecRegistry codecRegistry = com.mongodb.MongoClientSettings.getDefaultCodecRegistry();

    private UuidRepresentation uuidRepresentation = UuidRepresentation.JAVA_LEGACY;

    public MongoClient(final String hostURL) {
        this.databases = new HashMap<String, MongoDatabase>();
        this.hostURL = hostURL;
    }

    public MongoDatabase getDatabase(final String databaseName) {
        if (databases.containsKey(databaseName)) {
            return databases.get(databaseName);
        } else {
            databases.put(databaseName, new MongoDatabase(databaseName, hostURL));
            return databases.get(databaseName);
        }
    }
}
