package org.mongodb.async;

import org.mongodb.Document;
import org.mongodb.MongoClientURI;

import java.net.UnknownHostException;

/**
 * Helper class for asynchronous tests.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClientURI mongoClientURI;
    private static MongoClientImpl mongoClient;
    private static MongoDatabase defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            try {
                mongoClient = (MongoClientImpl) MongoClients.create(mongoURI, mongoURI.getOptions());
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURI.getURI(), e);
            }
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                                    ? DEFAULT_URI : mongoURIProperty;
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase("DriverTest-" + System.nanoTime());
        }
        return defaultDatabase;
    }

    public static MongoCollection<Document> initializeCollection(final MongoDatabase database, final String collectionName) {
        database.executeCommand(new Document("drop", collectionName));
        return database.getCollection(collectionName);
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (mongoClient != null) {
                if (defaultDatabase != null) {
                    defaultDatabase.executeCommand(new Document("dropDatabase", defaultDatabase.getName()));
                }
                mongoClient.close();
                mongoClient = null;
            }
        }
    }
}
