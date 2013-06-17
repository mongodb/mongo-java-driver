package org.mongodb

import spock.lang.Specification

import static org.mongodb.Fixture.getMongoClient
import static org.mongodb.Fixture.initialiseCollection

class FunctionalSpecification extends Specification {
    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;

    def setupSpec() {
        if (database == null) {
            database = getMongoClient().getDatabase('DriverTest-' + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
    }

    def setup() {
        collection = initialiseCollection(database, getClass().getName());
    }

    def cleanup() {
        if (collection != null) {
            collection.tools().drop();
        }
    }

    String getDatabaseName() {
        database.getName();
    }

    String getCollectionName() {
        collection.getName();
    }

    static class ShutdownHook extends Thread {
        @Override
        void run() {
            if (database != null) {
                database.tools().drop();
                getMongoClient().close();
            }
        }
    }

}
