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
import org.bson.BSONWriter;
import org.mongodb.CollectibleCodec;

public class PojoCodec<T> implements CollectibleCodec<T> {
    private Class<T> theClass;
    private PojoDecoder pojoDecoder;
    private PojoEncoder<T> pojoEncoder;

    public PojoCodec(final Codecs codecs, final Class<T> theClass) {
        this.theClass = theClass;
        pojoDecoder = new PojoDecoder(codecs);
        pojoEncoder = new PojoEncoder<T>(codecs);
    }

    @Override
    public Object getId(final Object document) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final T value) {
        pojoEncoder.encode(bsonWriter, value);
    }

    @Override
    public T decode(final BSONReader reader) {
        return pojoDecoder.decode(reader, theClass);
    }

    @Override
    public Class<T> getEncoderClass() {
        return theClass;
    }

}
