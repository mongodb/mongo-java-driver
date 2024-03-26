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
package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.mockito.MongoMockito.mock;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class ListCollectionsOperationTest {
    private ListCollectionsOperation<BsonDocument> operation;
    private Mocks mocks;

    @BeforeEach
    void beforeEach() {
        MongoNamespace namespace = new MongoNamespace("db", "coll");
        operation = new ListCollectionsOperation<>(namespace.getDatabaseName(), new BsonDocumentCodec());
        mocks = mocks(namespace);
    }

    @Test
    void executedCommandIsCorrect() {
        BsonDocument filter = new BsonDocument("key", new BsonString("value"));
        boolean nameOnly = true;
        boolean authorizedCollections = true;
        int batchSize = 123;
        BsonValue comment = new BsonString("comment");
        operation.filter(filter)
                .nameOnly(nameOnly)
                .authorizedCollections(authorizedCollections)
                .batchSize(batchSize)
                .comment(comment);
        assertEquals(
                new BsonDocument()
                        .append("listCollections", new BsonInt32(1))
                        .append("filter", filter)
                        .append("nameOnly", new BsonBoolean(nameOnly))
                        .append("authorizedCollections", new BsonBoolean(authorizedCollections))
                        .append("cursor", new BsonDocument()
                                .append("batchSize", new BsonInt32(batchSize))
                        )
                        .append("comment", comment),
                executeOperationAndCaptureCommand()
        );
    }

    @Test
    void authorizedCollectionsIsAbsentIfFalse() {
        operation.authorizedCollections(false);
        assertFalse(executeOperationAndCaptureCommand().containsKey("authorizedCollections"));
    }

    @Test
    void authorizedCollectionsIsFalseByDefault() {
        assertFalse(executeOperationAndCaptureCommand().containsKey("authorizedCollections"));
    }

    private BsonDocument executeOperationAndCaptureCommand() {
        operation.execute(mocks.readBinding());
        ArgumentCaptor<BsonDocument> commandCaptor = forClass(BsonDocument.class);
        verify(mocks.connection()).command(any(), commandCaptor.capture(), any(), any(), any(), any());
        return commandCaptor.getValue();
    }

    private static Mocks mocks(final MongoNamespace namespace) {
        Mocks result = new Mocks();
        result.readBinding(mock(ReadBinding.class, bindingMock -> {
            when(bindingMock.getOperationContext()).thenReturn(OPERATION_CONTEXT);
            ConnectionSource connectionSource = mock(ConnectionSource.class, connectionSourceMock -> {
                when(connectionSourceMock.getOperationContext()).thenReturn(OPERATION_CONTEXT);
                when(connectionSourceMock.release()).thenReturn(1);
                ServerAddress serverAddress = new ServerAddress();
                result.connection(mock(Connection.class, connectionMock -> {
                    when(connectionMock.release()).thenReturn(1);
                    ConnectionDescription connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), serverAddress));
                    when(connectionMock.getDescription()).thenReturn(connectionDescription);
                    when(connectionMock.command(any(), any(), any(), any(), any(), any())).thenReturn(cursorDoc(namespace));
                }));
                when(connectionSourceMock.getConnection()).thenReturn(result.connection());
                ServerDescription serverDescription = ServerDescription.builder()
                        .address(serverAddress)
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .build();
                when(connectionSourceMock.getServerDescription()).thenReturn(serverDescription);
                when(connectionSourceMock.getReadPreference()).thenReturn(ReadPreference.primary());
            });
            when(bindingMock.getReadConnectionSource()).thenReturn(connectionSource);
        }));
        return result;
    }

    private static BsonDocument cursorDoc(final MongoNamespace namespace) {
        return new BsonDocument()
                .append("cursor", new BsonDocument()
                        .append("firstBatch", new BsonArrayWrapper<BsonDocument>(emptyList()))
                        .append("ns", new BsonString(namespace.getFullName()))
                        .append("id", new BsonInt64(0))
                );
    }

    private static final class Mocks {
        @Nullable
        private ReadBinding readBinding;
        @Nullable
        private Connection connection;

        Mocks() {
        }

        void readBinding(final ReadBinding readBinding) {
            this.readBinding = readBinding;
        }

        ReadBinding readBinding() {
            return assertNotNull(readBinding);
        }

        void connection(final Connection connection) {
            this.connection = connection;
        }

        Connection connection() {
            return assertNotNull(connection);
        }
    }
}
