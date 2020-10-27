/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.singletonList;

final class FailPoint {
    private final MongoClient client;
    private final boolean ownsClient;
    private final BsonDocument failPointDocument;

    static FailPoint targeted(final BsonDocument operation, final Entities entities) {
        return new FailPoint(operation.getDocument("arguments").getDocument("failPoint"), createClient(operation, entities),
                true);
    }

    static FailPoint untargeted(final BsonDocument operation, final Entities entities) {
        return new FailPoint(operation.getDocument("arguments").getDocument("failPoint"),
                entities.getClient(operation.getDocument("arguments").getString("client").getValue()), false);
    }

    private FailPoint(final BsonDocument failPointDocument, final MongoClient client, final boolean ownsClient) {
        this.client = client;
        this.ownsClient = ownsClient;
        this.failPointDocument = failPointDocument;
    }

    void executeFailPoint() {
        client.getDatabase("admin").runCommand(failPointDocument);
    }

    void disableFailPoint() {
        client.getDatabase("admin")
                .runCommand(new BsonDocument("configureFailPoint",
                        failPointDocument.getString("configureFailPoint"))
                        .append("mode", new BsonString("off")));
        if (ownsClient) {
            client.close();
        }
    }

    private static MongoClient createClient(final BsonDocument operation, final Entities entities) {
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession clientSession = entities.getSession(arguments.getString("session").getValue());

        if (clientSession.getPinnedServerAddress() == null) {
            throw new UnsupportedOperationException("Can't target a failpoint to a server where the session is not pinned");
        }

        return MongoClients.create(getMongoClientSettingsBuilder()
                .applyToClusterSettings(builder -> builder.hosts(singletonList(clientSession.getPinnedServerAddress()))).build());
    }
}
