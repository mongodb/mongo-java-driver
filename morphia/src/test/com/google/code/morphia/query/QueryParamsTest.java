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

package com.google.code.morphia.query;

import com.google.code.morphia.MappingTest.BaseEntity;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.testutil.AssertedFailure;
import org.junit.Test;

import java.util.Collections;

public class QueryParamsTest extends TestBase {
    @Entity
    static class E extends BaseEntity {

    }

    @Test
    public void testNullAcceptance() throws Exception {
        final Query<E> q = ds.createQuery(E.class);
        final FieldEnd<?> e = q.field("_id");

        // have to suceed:
        e.equal(null);
        e.notEqual(null);
        e.hasThisOne(null);

        // have to fail:
        new AssertedFailure() {
            public void thisMustFail() {
                e.greaterThan(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.greaterThanOrEq(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasAllOf(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasAnyOf(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasNoneOf(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasThisElement(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.lessThan(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.lessThanOrEq(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.startsWith(null);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.startsWithIgnoreCase(null);
            }
        };
    }

    @Test
    public void testEmptyCollectionAcceptance() throws Exception {
        final Query<E> q = ds.createQuery(E.class);
        final FieldEnd<?> e = q.field("_id");

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasAllOf(Collections.EMPTY_LIST);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() {
                e.hasNoneOf(Collections.EMPTY_LIST);
            }
        };

        //        new AssertedFailure() {
        //            public void thisMustFail() throws Throwable {
        //                e.hasAnyOf(Collections.EMPTY_LIST);
        //            }
        //        };
    }
}
