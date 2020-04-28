package com.mongodb.client;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getOcspShouldSucceed;

import static org.junit.Assert.fail;

public class OcspTest {
    @Test
    public void testTLS() {
        String uri = "mongodb://localhost/?serverSelectionTimeoutMS=2000&tls=true";
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
        } catch (MongoTimeoutException e) {
            if (getOcspShouldSucceed()) {
                fail("Unexpected exception when using OCSP with tls=true: " + e);
            }
        }
    }
}
