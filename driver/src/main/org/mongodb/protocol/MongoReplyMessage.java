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

import org.bson.BSONBinaryReader;
import org.bson.BSONReader;
import org.bson.BSONReaderSettings;
import org.bson.io.InputBuffer;
import org.mongodb.Decoder;
import org.mongodb.MongoInternalException;
import org.mongodb.io.ResponseBuffers;

import java.util.ArrayList;
import java.util.List;

public class MongoReplyMessage<T> {

    private final MongoReplyHeader replyHeader;
    private final long elapsedNanoseconds;
    private final List<T> documents;

    public MongoReplyMessage(final MongoReplyHeader replyHeader, final InputBuffer bodyInputBuffer,
                             final Decoder<T> decoder, final long requestId, final long elapsedNanoseconds) {
        this(replyHeader, requestId, elapsedNanoseconds);

        while (documents.size() < replyHeader.getNumberReturned()) {
            final BSONReader reader = new BSONBinaryReader(new BSONReaderSettings(), bodyInputBuffer);
            documents.add(decoder.decode(reader));
            reader.close();
        }
    }

    public MongoReplyMessage(final MongoReplyHeader replyHeader, final long requestId, final long elapsedNanoseconds) {
        if (requestId != replyHeader.getResponseTo()) {
            throw new MongoInternalException(
                    String.format("The responseTo (%d) in the response does not match the requestId (%d) in the request",
                    replyHeader.getResponseTo(), requestId));
        }

        this.replyHeader = replyHeader;
        this.elapsedNanoseconds = elapsedNanoseconds;

        documents = new ArrayList<T>(replyHeader.getNumberReturned());
    }

    public MongoReplyMessage(final ResponseBuffers responseBuffers, final Decoder<T> decoder, final long requestId) {
        this(responseBuffers.getReplyHeader(), responseBuffers.getBodyByteBuffer(), decoder, requestId,
                responseBuffers.getElapsedNanoseconds());
    }

    public MongoReplyHeader getReplyHeader() {
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
