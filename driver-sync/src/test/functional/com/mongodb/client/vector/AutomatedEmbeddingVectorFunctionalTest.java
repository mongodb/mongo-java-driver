package com.mongodb.client.vector;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class AutomatedEmbeddingVectorFunctionalTest extends AbstractAutomatedEmbeddingVectorSearchFunctionalTest {
    @Override
    protected MongoClient getMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }
}
