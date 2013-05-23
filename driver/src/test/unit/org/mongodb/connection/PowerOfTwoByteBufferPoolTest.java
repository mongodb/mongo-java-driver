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

package org.mongodb.connection;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class PowerOfTwoByteBufferPoolTest {
    private PowerOfTwoByteBufferPool pool;

    @Before
    public void setUp() {
        pool = new PowerOfTwoByteBufferPool(10);
    }

    @Test
    public void testNormalRequest() {
        ByteBuffer buf = pool.get((int) Math.pow(2, 10));
        assertEquals((int) Math.pow(2, 10), buf.capacity());
        assertEquals((int) Math.pow(2, 10), buf.limit());

        buf = pool.get((int) Math.pow(2, 10) - 1);
        assertEquals((int) Math.pow(2, 10), buf.capacity());
        assertEquals((int) Math.pow(2, 10) - 1, buf.limit());
    }

    @Test
    public void testReuse() {
        ByteBuffer buf = pool.get((int) Math.pow(2, 10));
        pool.release(buf);
        assertSame(buf, pool.get((int) Math.pow(2, 10)));
    }

    @Test
    public void testHugeBufferRequest() {
        ByteBuffer buf = pool.get((int) Math.pow(2, 10) + 1);
        assertEquals((int) Math.pow(2, 10) + 1, buf.capacity());
        assertEquals((int) Math.pow(2, 10) + 1, buf.limit());

        pool.release(buf);
        assertNotSame(buf, pool.get((int) Math.pow(2, 10) + 1));
    }
}
