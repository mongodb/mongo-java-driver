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

package com.mongodb;

import org.junit.Test;
import org.mongodb.connection.Tags;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ReadPreferenceTest {

    @Test
    public void testFromNew() {
        assertEquals(ReadPreference.primary(), ReadPreference.fromNew(org.mongodb.ReadPreference.primary()));
        assertEquals(ReadPreference.secondary(), ReadPreference.fromNew(org.mongodb.ReadPreference.secondary()));
    }

    @Test
    public void testTags() {
        ReadPreference secondary = ReadPreference.secondary(new BasicDBObject("dc", "ny"));
        assertEquals(Arrays.asList(new Tags("dc", "ny")), ((org.mongodb.TaggableReadPreference) secondary.toNew()).getTagsList());
    }

    @Test
    public void testTagsList() {
        ReadPreference secondary = ReadPreference.secondary(new BasicDBObject("dc", "ny").append("rack", "1"),
                new BasicDBObject("dc", "ca").append("rack", "1"));
        Tags tags1 = new Tags("dc", "ny").append("rack", "1");
        Tags tags2 = new Tags("dc", "ca").append("rack", "1");
        assertEquals(Arrays.asList(tags1, tags2), ((org.mongodb.TaggableReadPreference) secondary.toNew()).getTagsList());
    }
}
