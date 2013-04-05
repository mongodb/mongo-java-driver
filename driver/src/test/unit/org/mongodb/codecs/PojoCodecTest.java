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

import org.bson.BSONWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PojoCodecTest {
    private PojoCodec pojoCodec;
    private BSONWriter bsonWriter = mock(BSONWriter.class);
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();

    @Before
    public void setUp() {
        pojoCodec = new PojoCodec(primitiveCodecs);
    }

    @Test
    public void shouldEncodeString() {
        pojoCodec.encode(bsonWriter, "Bob");

        verify(bsonWriter).writeString("Bob");
    }

    @Test
    public void shouldEncodeInt() {
        pojoCodec.encode(bsonWriter, 32);

        verify(bsonWriter).writeInt32(32);
    }

    @Test
    @Ignore("Not implemented")
    public void shouldEncodeSimplePojo() {
        Assert.fail("not implemented");
    }

}
