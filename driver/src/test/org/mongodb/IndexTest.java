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

package org.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IndexTest {

    @Test
    public void shouldGenerateIndexNameForSimpleKey() {
        final Index index = new Index("x");
        assertEquals("x_1", index.getName());
    }

    @Test
    public void shouldGenerateIndexNameForKeyOrderedAscending() {
        final Index index = new Index("x", OrderBy.ASC);
        assertEquals("x_1", index.getName());
    }

    @Test
    public void shouldGenerateIndexNameForKeyOrderedDescending() {
        final Index index = new Index("x", OrderBy.DESC);
        assertEquals("x_-1", index.getName());
    }

    @Test
    public void shouldGenerateGeoIndexName() {
        final Index index = new Index(new Index.GeoKey("x"));
        assertEquals("x_2d", index.getName());
    }

    @Test
    public void shouldCompoundIndexName() {
        final Index index = new Index(new Index.OrderedKey("x", OrderBy.ASC),
                               new Index.OrderedKey("y", OrderBy.ASC),
                               new Index.OrderedKey("a", OrderBy.ASC));
        assertEquals("x_1_y_1_a_1", index.getName());

    }

    @Test
    public void shouldGenerateGeoAndSortedCompoundIndexName() {
        final Index index = new Index(new Index.GeoKey("x"),
                               new Index.OrderedKey("y", OrderBy.DESC));
        assertEquals("x_2d_y_-1", index.getName());

    }

}
