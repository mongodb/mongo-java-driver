package org.mongodb.async

import org.mongodb.Document
import org.mongodb.MongoCommandFailureException
import org.mongodb.MongoNamespace
import spock.lang.Specification

import static org.mongodb.async.Fixture.getDefaultDatabase
import static org.mongodb.async.Fixture.initializeCollection


class FunctionalSpecification extends Specification {
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;

    def setup() {
        database = getDefaultDatabase()
        collection = initializeCollection(database, getClass().getName())
    }

    def cleanup() {
        if (collection != null) {
            try {
                database.executeCommand(new Document('drop', collection.getName())).get()
            } catch (MongoCommandFailureException e) {
                if (!e.getErrorMessage().startsWith('ns not found')) {
                    throw e;
                }
            }

        }
    }

    String getDatabaseName() {
        database.getName();
    }

    String getCollectionName() {
        collection.getName();
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }
}
