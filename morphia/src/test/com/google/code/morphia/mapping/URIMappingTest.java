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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ScottHernandez
 */
@SuppressWarnings("unused")
public class URIMappingTest extends TestBase {

    private static class ContainsURI {
        @Id
        private ObjectId id;
        private URI uri;
    }

    private static class ContainsURIKeyedMap {
        @Id
        private ObjectId id;
        private final Map<URI, String> uris = new HashMap<URI, String>();
    }

    @Test
    public void testURIField() throws Exception {
        final ContainsURI entity = new ContainsURI();
        final URI testURI = new URI("http://lamest.local/test.html");

        entity.uri = testURI;
        ds.save(entity);
        final ContainsURI loaded = ds.find(ContainsURI.class).get();
        Assert.assertNotNull(loaded.uri);
        Assert.assertEquals(testURI, loaded.uri);
    }

    @Test
    public void testURIMap() throws Exception {
        final ContainsURIKeyedMap entity = new ContainsURIKeyedMap();
        final URI testURI = new URI("http://lamest.local/test.html");

        entity.uris.put(testURI, "first");
        ds.save(entity);
        final ContainsURIKeyedMap loaded = ds.find(ContainsURIKeyedMap.class).get();
        Assert.assertNotNull(loaded.uris);
        Assert.assertEquals(1, loaded.uris.size());
        Assert.assertEquals(testURI, loaded.uris.keySet().iterator().next());

    }
}
