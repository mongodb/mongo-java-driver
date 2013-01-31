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

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author scotthernandez
 */
@SuppressWarnings("unused")
public class QueryInForReferencedListTest extends TestBase {

    @Entity
    private static class HasRefs {
        private static final long serialVersionUID = 1L;

        @Id
        private final ObjectId id = new ObjectId();
        @Reference
        private final ArrayList<ReferencedEntity> refs = new ArrayList<QueryInForReferencedListTest.ReferencedEntity>();
    }

    @Entity
    private static class ReferencedEntity extends TestEntity {
        private static final long serialVersionUID = 1L;
        private String foo;

        public ReferencedEntity(final String s) {
            foo = s;
        }

        public ReferencedEntity() {
        }

    }

    @Entity("docs")
    private static class Doc {
        @Id
        private long id = 4;
    }

    @Test
    public void testMapping() throws Exception {

        morphia.map(HasRefs.class);
        morphia.map(ReferencedEntity.class);
    }

    @Test
    public void testInQuery() throws Exception {
        final HasRefs hr = new HasRefs();
        for (int x = 0; x < 10; x++) {
            final ReferencedEntity re = new ReferencedEntity("" + x);
            hr.refs.add(re);
        }
        ds.save(hr.refs);
        ds.save(hr);

        final List<HasRefs> res = ds.createQuery(HasRefs.class).field("refs")
                                    .in(hr.refs.subList(1, 3)).asList();
        Assert.assertEquals(1, res.size());
    }

    @Test
    public void testInQuery2() throws Exception {
        final Doc doc = new Doc();
        doc.id = 1;
        ds.save(doc);

        // this works
        final List<Doc> docs1 = ds.find(Doc.class).field("_id").equal(1).asList();

        final List<Long> idList = new ArrayList<Long>();
        idList.add(1L);
        // this causes an NPE
        final List<Doc> docs2 = ds.find(Doc.class).field("_id").in(idList).asList();

    }

}
