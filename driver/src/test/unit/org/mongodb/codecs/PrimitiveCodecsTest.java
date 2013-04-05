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

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;
import java.util.regex.Pattern;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PrimitiveCodecsTest {
    private final PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();

    @Test
    public void shouldBeAbleToSerializeString() {
        assertThat(primitiveCodecs.canEncode(String.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeObjectId() {
        assertThat(primitiveCodecs.canEncode(ObjectId.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeInteger() {
        System.out.println(Integer.class);
        assertThat(primitiveCodecs.canEncode(Integer.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeLong() {
        assertThat(primitiveCodecs.canEncode(Long.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeDouble() {
        assertThat(primitiveCodecs.canEncode(Double.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeBinary() {
        assertThat(primitiveCodecs.canEncode(Binary.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeDate() {
        assertThat(primitiveCodecs.canEncode(Date.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeTimestamp() {
        assertThat(primitiveCodecs.canEncode(BSONTimestamp.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeBoolean() {
        assertThat(primitiveCodecs.canEncode(Boolean.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializePattern() {
        assertThat(primitiveCodecs.canEncode(Pattern.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeMinKey() {
        assertThat(primitiveCodecs.canEncode(MinKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeMaxKey() {
        assertThat(primitiveCodecs.canEncode(MaxKey.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeCode() {
        assertThat(primitiveCodecs.canEncode(Code.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeNull() {
        assertThat(primitiveCodecs.canEncode(null), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeFloat() {
        assertThat(primitiveCodecs.canEncode(Float.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeShort() {
        assertThat(primitiveCodecs.canEncode(Short.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeByte() {
        assertThat(primitiveCodecs.canEncode(Byte.class), is(true));
    }

    @Test
    public void shouldBeAbleToSerializeByteArray() {
        assertThat(primitiveCodecs.canEncode(byte[].class), is(true));
    }

}
