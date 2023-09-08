/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.lambdatest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.lang.NonNull;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Test App for AWS lambda functions
 */
public class LambdaTestApp implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private final MongoClient mongoClient;
    private long openConnections = 0;
    private long totalHeartbeatCount = 0;
    private long totalHeartbeatDurationMs = 0;
    private long totalCommandCount = 0;
    private long totalCommandDurationMs = 0;

    public LambdaTestApp() {
        String connectionString = System.getenv("MONGODB_URI");

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandSucceeded(@NonNull final CommandSucceededEvent event) {
                        totalCommandCount++;
                        totalCommandDurationMs += event.getElapsedTime(MILLISECONDS);
                    }
                    @Override
                    public void commandFailed(@NonNull final CommandFailedEvent event) {
                        totalCommandCount++;
                        totalCommandDurationMs += event.getElapsedTime(MILLISECONDS);
                    }
                })
                .applyToServerSettings(builder -> builder.addServerMonitorListener(new ServerMonitorListener() {
                    @Override
                    public void serverHeartbeatSucceeded(@NonNull final ServerHeartbeatSucceededEvent event) {
                        totalHeartbeatCount++;
                        totalHeartbeatDurationMs += event.getElapsedTime(MILLISECONDS);
                    }
                    @Override
                    public void serverHeartbeatFailed(@NonNull final ServerHeartbeatFailedEvent event) {
                        totalHeartbeatCount++;
                        totalHeartbeatDurationMs += event.getElapsedTime(MILLISECONDS);
                    }
                }))
                .applyToConnectionPoolSettings(builder -> builder.addConnectionPoolListener(new ConnectionPoolListener() {
                    @Override
                    public void connectionCreated(@NonNull final ConnectionCreatedEvent event) {
                        openConnections++;
                    }
                    @Override
                    public void connectionClosed(@NonNull final ConnectionClosedEvent event) {
                        openConnections--;
                    }
                }))
                .build();
        mongoClient = MongoClients.create(settings);
    }

    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        try {
            MongoCollection<Document> collection = mongoClient
                    .getDatabase("lambdaTest")
                    .getCollection("test");
            BsonValue id = collection.insertOne(new Document("n", 1)).getInsertedId();
            collection.deleteOne(new Document("_id", id));

            BsonDocument responseBody = new BsonDocument()
                    .append("totalCommandDurationMs", new BsonInt64(totalCommandDurationMs))
                    .append("totalCommandCount", new BsonInt64(totalCommandCount))
                    .append("totalHeartbeatDurationMs", new BsonInt64(totalHeartbeatDurationMs))
                    .append("totalHeartbeatCount", new BsonInt64(totalHeartbeatCount))
                    .append("openConnections", new BsonInt64(openConnections));

            return templateResponse()
                    .withStatusCode(200)
                    .withBody(responseBody.toJson());

        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            BsonDocument responseBody = new BsonDocument()
                    .append("throwable", new BsonString(e.getMessage()))
                    .append("stacktrace", new BsonString(sw.toString()));
            return templateResponse()
                    .withBody(responseBody.toJson())
                    .withStatusCode(500);
        }
    }

    private APIGatewayProxyResponseEvent templateResponse() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        return new APIGatewayProxyResponseEvent()
                .withHeaders(headers);
    }
}
