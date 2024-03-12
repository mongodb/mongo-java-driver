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

package com.mongodb.internal.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.FieldNameValidator;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.THREE_DOT_SIX_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CommandMessageTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));
    private static final FieldNameValidator FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();

    @Test
    void shouldThrowTimeoutExceptionWhenTimeoutContextIsCalled() {
        //given
        CommandMessage commandMessage = new CommandMessage(NAMESPACE, COMMAND, FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(THREE_DOT_SIX_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_SECONDARY)
                        .sessionSupported(true)
                        .build(),
                true, null, null, ClusterConnectionMode.MULTIPLE, null);

        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        SessionContext sessionContext = mock(SessionContext.class);
        TimeoutContext timeoutContext = mock(TimeoutContext.class, mock -> {
            doThrow(new MongoOperationTimeoutException("test")).when(mock).addExtraElements(anyList());
        });
        OperationContext operationContext = mock(OperationContext.class, mock -> {
            when(mock.getSessionContext()).thenReturn(sessionContext);
            when(mock.getTimeoutContext()).thenReturn(timeoutContext);
        });


        //when & then
        assertThrows(MongoOperationTimeoutException.class, () ->
                commandMessage.encode(bsonOutput, operationContext));
    }

    @Test
    void shouldNotAddExtraElementsFromTimeoutContextWhenConnectedToMongoCrypt() {
        //given
        CommandMessage commandMessage = new CommandMessage(NAMESPACE, COMMAND, FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(THREE_DOT_SIX_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_SECONDARY)
                        .sessionSupported(true)
                        .cryptd(true)
                        .build(),
                true, null, null, ClusterConnectionMode.MULTIPLE, null);

        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        SessionContext sessionContext = mock(SessionContext.class, mock -> {
            when(mock.getClusterTime()).thenReturn(new BsonDocument("clusterTime", new BsonTimestamp(42, 1)));
            when(mock.hasSession()).thenReturn(false);
            when(mock.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
            when(mock.notifyMessageSent()).thenReturn(true);
            when(mock.hasActiveTransaction()).thenReturn(false);
            when(mock.isSnapshot()).thenReturn(false);
        });
        TimeoutContext timeoutContext = mock(TimeoutContext.class);
        OperationContext operationContext = mock(OperationContext.class, mock -> {
            when(mock.getSessionContext()).thenReturn(sessionContext);
            when(mock.getTimeoutContext()).thenReturn(timeoutContext);
        });

        //when
        commandMessage.encode(bsonOutput, operationContext);

        //then
        verifyNoInteractions(timeoutContext);
    }
}
