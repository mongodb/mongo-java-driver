/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import org.bson.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class PowerOfTwoBufferPoolTest {
    private PowerOfTwoBufferPool pool;

    @Before
    public void setUp() {
        pool = new PowerOfTwoBufferPool(10);
    }

    @Test
    public void testNormalRequest() {

        for (int i = 0; i <= 10; i++) {
            ByteBuf buf = pool.getBuffer((int) Math.pow(2, i));
            assertEquals((int) Math.pow(2, i), buf.capacity());
            assertEquals((int) Math.pow(2, i), buf.limit());

            if (i > 1) {
                buf = pool.getBuffer((int) Math.pow(2, i) - 1);
                assertEquals((int) Math.pow(2, i), buf.capacity());
                assertEquals((int) Math.pow(2, i) - 1, buf.limit());
            }

            if (i < 10) {
                buf = pool.getBuffer((int) Math.pow(2, i) + 1);
                assertEquals((int) Math.pow(2, i + 1), buf.capacity());
                assertEquals((int) Math.pow(2, i) + 1, buf.limit());
            }
        }
    }

    @Test
    public void testReuse() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, 10));
        ByteBuffer byteBuffer = buf.asNIO();
        buf.release();
        assertSame(byteBuffer, pool.getBuffer((int) Math.pow(2, 10)).asNIO());
    }

    @Test
    public void testHugeBufferRequest() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, 10) + 1);
        assertEquals((int) Math.pow(2, 10) + 1, buf.capacity());
        assertEquals((int) Math.pow(2, 10) + 1, buf.limit());

        buf.release();
        assertNotSame(buf, pool.getBuffer((int) Math.pow(2, 10) + 1));
    }
}
