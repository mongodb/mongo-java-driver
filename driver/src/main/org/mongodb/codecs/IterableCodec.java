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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.mongodb.Codec;
import org.mongodb.Decoder;

import java.util.Collection;

public class IterableCodec implements Codec<Iterable> {
    private final CollectionFactory collectionFactory;
    private Codecs codecs;
    private Decoder<?> decoder;

    public IterableCodec(final Codecs codecs) {
        this(codecs, new ArrayListFactory(), codecs);
    }

    public IterableCodec(final Codecs codecs, final CollectionFactory collectionFactory, final Decoder<?> decoder) {
        this.codecs = codecs;
        this.collectionFactory = collectionFactory;
        this.decoder = decoder;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Iterable iterable) {
        bsonWriter.writeStartArray();
        for (Object value : iterable) {
            codecs.encode(bsonWriter, value);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public <E> Iterable<E> decode(final BSONReader reader) {
        reader.readStartArray();
        final Collection<E> collection = collectionFactory.createCollection();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            //TODO: This is the only bit that's dangerous, I think
            collection.add((E) decoder.decode(reader));
        }
        reader.readEndArray();
        return collection;
    }

    @Override
    public Class<Iterable> getEncoderClass() {
        return Iterable.class;
    }
}
