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

    private final MongoReplyHeader replyHeader;
    private final List<T> documents;

    public MongoReplyMessage(final MongoReplyHeader replyHeader, final InputBuffer bodyInputBuffer,
                             final Serializer<T> serializer) {
        this.replyHeader = replyHeader;

        documents = new ArrayList<T>(replyHeader.getNumberReturned());

        while (documents.size() < replyHeader.getNumberReturned()) {
            final BSONReader reader = new BSONBinaryReader(new BsonReaderSettings(), bodyInputBuffer);
            documents.add(serializer.deserialize(reader));
            reader.close();
        }
    }

    public MongoReplyHeader getReplyHeader() {
        return replyHeader;
    }

    public List<T> getDocuments() {
        return documents;
    }
}
