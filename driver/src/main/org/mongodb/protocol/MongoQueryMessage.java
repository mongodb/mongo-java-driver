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

import org.mongodb.Document;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoFind;
import org.mongodb.Encoder;

public class MongoQueryMessage extends MongoQueryBaseMessage {
    private final MongoFind find;
    private Encoder<Document> encoder;

    public MongoQueryMessage(final String collectionName, final MongoFind find, final Encoder<Document> encoder) {
        super(collectionName);
        this.find = find;
        this.encoder = encoder;
    }

    @Override
    protected void encodeMessageBody(final ChannelAwareOutputBuffer buffer) {
        writeQueryPrologue(find, buffer);
        addDocument(getQueryDocument(), encoder, buffer);
        if (find.getFields() != null) {
            addDocument(find.getFields(), encoder, buffer);
        }
    }

    private Document getQueryDocument() {
        final Document document = new Document();
        document.put("query", find.getFilter());
        if (find.getOrder() != null && !find.getOrder().isEmpty()) {
            document.put("orderby", find.getOrder());
        }
        if (find.isSnapshotMode()) {
            document.put("$snapshot", true);
        }
        // TODO: only to mongos according to spec
        if (find.getReadPreference() != null) {
            document.put("$readPreference", find.getReadPreference().toDocument());
        }
        // TODO: explain and hint
        return document;
    }
}
