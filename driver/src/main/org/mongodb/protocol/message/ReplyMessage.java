/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.protocol.message;

import org.bson.BsonBinaryReader;
import org.bson.BsonReaderSettings;
import org.bson.codecs.Decoder;
import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.ReplyHeader;
import org.mongodb.connection.ResponseBuffers;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class ReplyMessage<T> {

    private final ReplyHeader replyHeader;
    private final long elapsedNanoseconds;
    private final List<T> documents;

    public ReplyMessage(final ReplyHeader replyHeader, final long requestId, final long elapsedNanoseconds) {
        if (requestId != replyHeader.getResponseTo()) {
            throw new MongoInternalException(format("The responseTo (%d) in the response does not match the requestId (%d) in the "
                                                    + "request", replyHeader.getResponseTo(), requestId));
        }

        this.replyHeader = replyHeader;
        this.elapsedNanoseconds = elapsedNanoseconds;

        documents = new ArrayList<T>(replyHeader.getNumberReturned());
    }

    public ReplyMessage(final ResponseBuffers responseBuffers, final Decoder<T> decoder, final long requestId) {
        this(responseBuffers.getReplyHeader(), requestId, responseBuffers.getElapsedNanoseconds());

        if (replyHeader.getNumberReturned() > 0) {
            InputBuffer inputBuffer = new BasicInputBuffer(responseBuffers.getBodyByteBuffer());
            while (documents.size() < replyHeader.getNumberReturned()) {
                BsonBinaryReader reader = new BsonBinaryReader(new BsonReaderSettings(), inputBuffer, false);
                try {
                    documents.add(decoder.decode(reader));
                } finally {
                    reader.close();
                }
            }
        }
    }

    public ReplyHeader getReplyHeader() {
        return replyHeader;
    }

    public List<T> getDocuments() {
        return documents;
    }

    /**
     * The number of nanoseconds elapses since the message that this is a reply to was sent.
     *
     * @return elapsed nanoseconds
     */
    public long getElapsedNanoseconds() {
        return elapsedNanoseconds;
    }
}
