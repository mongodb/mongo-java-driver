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

import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.codecs.Decoder;

abstract class CommandResultBaseCallback<T> extends ResponseCallback {
    public static final Logger LOGGER = Loggers.getLogger("protocol.command");
    private final Decoder<T> decoder;

    CommandResultBaseCallback(final Decoder<T> decoder, final long requestId, final ServerAddress serverAddress) {
        super(requestId, serverAddress);
        this.decoder = decoder;
    }

    protected void callCallback(final ResponseBuffers responseBuffers, final Throwable t) {
        try {
            if (t != null || responseBuffers == null) {
                callCallback((T) null, t);
            } else {
                ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, decoder, getRequestId());
                callCallback(replyMessage.getDocuments().get(0), null);
            }
        } finally {
            try {
                if (responseBuffers != null) {
                    responseBuffers.close();
                }
            } catch (Throwable t1) {
                LOGGER.debug("GetMore ResponseBuffer close exception", t1);
            }
        }
    }

    protected abstract void callCallback(T response, Throwable t);
}
