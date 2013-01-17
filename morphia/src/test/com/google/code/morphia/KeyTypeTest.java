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

package com.google.code.morphia;

import com.google.code.morphia.testmodel.Rectangle;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author Scott Hernandez
 */
@SuppressWarnings({"unused"})
public class KeyTypeTest extends TestBase {
    @Test
    public void testKeyComparisons() throws Exception {
        final Rectangle r = new Rectangle(2, 1);
        final Key<Rectangle> k1 = new Key<Rectangle>(Rectangle.class, r.getId());
        final Key<Rectangle> k2 = this.ds.getKey(r);

        Assert.assertTrue(k1.equals(k2));
        Assert.assertTrue(k2.equals(k1));

    }

}
