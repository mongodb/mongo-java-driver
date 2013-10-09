/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.protocol;

import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoException;
import org.mongodb.MongoWriteException;
import org.mongodb.WriteResult;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerDescription;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.Arrays;
import java.util.List;

final class ProtocolHelper {
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);

    static MongoException getWriteException(final WriteResult writeResult) {
        if (!writeResult.getCommandResult().isOk()) {
            return new MongoCommandFailureException(writeResult.getCommandResult());
        } else if (writeResult.getErrorMessage() != null) {
            if (DUPLICATE_KEY_ERROR_CODES.contains(writeResult.getCommandResult().getErrorCode())) {
                return new MongoDuplicateKeyException(writeResult);
            } else {
                return new MongoWriteException(writeResult);
            }
        } else {
            return null;
        }
    }

    public static MessageSettings getMessageSettings(final ServerDescription serverDescription) {
        return MessageSettings.builder()
                              .maxDocumentSize(serverDescription.getMaxDocumentSize())
                              .maxMessageSize(serverDescription.getMaxMessageSize())
                              .build();
    }

    public static RequestMessage encodeMessageToBuffer(final RequestMessage message, final PooledByteBufferOutputBuffer buffer) {
        try {
            return message.encode(buffer);
        } catch (RuntimeException e) {
            buffer.close();
            throw e;
        } catch (Error e) {
            buffer.close();
            throw e;
        }
    }

    private ProtocolHelper() {
    }
}
