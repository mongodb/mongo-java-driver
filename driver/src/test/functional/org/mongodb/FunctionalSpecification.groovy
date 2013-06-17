package org.mongodb

import spock.lang.Specification

class FunctionalSpecification extends Specification {
    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected String collectionName;

    def setupSpec() {
        if (database == null) {
            database = Fixture.getMongoClient().getDatabase('DriverTest-' + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
    }

    def setup() {
        collectionName = getClass().getName();
        collection = database.getCollection(collectionName);
        collection.tools().drop();
    }

    def cleanup() {
        if (collection != null) {
            collection.tools().drop();
        }
    }

    protected String getDatabaseName() {
        database.getName();
    }

    static class ShutdownHook extends Thread {
        @Override
        void run() {
            if (database != null) {
                database.tools().drop();
                Fixture.getMongoClient().close();
            }
        }
    }

}
