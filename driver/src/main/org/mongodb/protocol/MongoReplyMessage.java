/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import org.bson.BsonReaderSettings;
import org.bson.io.InputBuffer;
import org.mongodb.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;

public class MongoReplyMessage<T> {
    private final int messageLength;
    private final int requestId;
    private final int responseTo;
    private final int responseFlags;
    private final long cursorId;
    private final int startingFrom;
    private final int numberReturned;
    private final List<T> documents;

    public MongoReplyMessage(final InputBuffer headerInputBuffer, final InputBuffer bodyInputBuffer,
                             final Serializer<T> serializer) {
        messageLength = headerInputBuffer.readInt32();
        requestId = headerInputBuffer.readInt32();
        responseTo = headerInputBuffer.readInt32();  // TODO: validate that this is a response to the expected message
        final int opCode = headerInputBuffer.readInt32();  // TODO: check for validity
        responseFlags = headerInputBuffer.readInt32();
        cursorId = headerInputBuffer.readInt64();
        startingFrom = headerInputBuffer.readInt32();
        numberReturned = headerInputBuffer.readInt32();

        documents = new ArrayList<T>(numberReturned);

        while (documents.size() < numberReturned) {
            final BSONReader reader = new BSONBinaryReader(new BsonReaderSettings(), bodyInputBuffer);
            documents.add(serializer.deserialize(reader, null));
            reader.close();
        }
    }

    public int getMessageLength() {
        return messageLength;
    }

    public int getRequestId() {
        return requestId;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public int getResponseFlags() {
        return responseFlags;
    }

    public long getCursorId() {
        return cursorId;
    }

    public int getStartingFrom() {
        return startingFrom;
    }

    public int getNumberReturned() {
        return numberReturned;
    }

    public List<T> getDocuments() {
        return documents;
    }
}
