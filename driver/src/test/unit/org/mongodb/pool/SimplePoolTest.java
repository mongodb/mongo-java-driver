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

package org.mongodb.pool;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SimplePoolTest {
    private SimplePool<ByteBuffer> pool;

    @Before
    public void setUp() {
       pool = new SimplePool<ByteBuffer>("test", 5) {
           @Override
           protected ByteBuffer createNew() {
               return ByteBuffer.allocate(10);
           }
       };
    }

    // this is just testing that you can call done after clear without getting an error
    @Test
    public void testDoneAfterClear() {
        ByteBuffer buffer = pool.get();
        pool.clear();
        pool.done(buffer);
    }
}
