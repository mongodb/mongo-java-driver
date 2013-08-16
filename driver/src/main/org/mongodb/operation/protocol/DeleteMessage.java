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

package org.mongodb.operation.protocol;

import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.operation.Remove;

import java.util.List;

public class DeleteMessage extends RequestMessage {
    private final List<Remove> removes;
    private final Encoder<Document> encoder;

    public DeleteMessage(final String collectionName, final List<Remove> removes, final Encoder<Document> encoder,
                         final MessageSettings settings) {
        super(collectionName, OpCode.OP_DELETE, settings);
        this.removes = removes;
        this.encoder = encoder;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        writeDelete(removes.get(0), buffer);
        if (removes.size() == 1) {
            return null;
        }
        else {
            return new DeleteMessage(getCollectionName(), removes.subList(1, removes.size()), encoder, getSettings());
        }
    }

    private void writeDelete(final Remove remove, final OutputBuffer buffer) {
        buffer.writeInt(0); // reserved
        buffer.writeCString(getCollectionName());

        if (remove.isMulti()) {
            buffer.writeInt(0);
        }
        else {
            buffer.writeInt(1);
        }

        addDocument(remove.getFilter(), encoder, buffer);
    }
}

