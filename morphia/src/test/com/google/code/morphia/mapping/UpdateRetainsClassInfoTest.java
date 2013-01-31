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

package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UpdateRetainsClassInfoTest extends TestBase {
    public abstract static class E {
        @Id
        private final ObjectId id = new ObjectId();
    }

    public static class E1 extends E {
        private String foo;
    }

    public static class E2 extends E {
        private String bar;
    }

    public static class X {
        @Id
        private ObjectId id;
        private final Map<String, E> map = new HashMap<String, E>();

    }

    @Test
    public void retainsClassName() {
        X x = new X();

        final E1 e1 = new E1();
        e1.foo = "narf";
        x.map.put("k1", e1);

        final E2 e2 = new E2();
        e2.bar = "narf";
        x.map.put("k2", e2);

        ds.save(x);

        final Query<X> stateQuery = ds.createQuery(X.class);
        final UpdateOperations<X> stateUpdate = ds.createUpdateOperations(X.class);
        stateUpdate.set("map.k2", e2);

        ds.update(stateQuery, stateUpdate);

        // fails due to type now missing
        x = ds.find(X.class).get();
    }
}