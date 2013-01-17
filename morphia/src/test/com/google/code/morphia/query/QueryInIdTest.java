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

package com.google.code.morphia.query;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author scotthernandez
 */
@SuppressWarnings("unused")
public class QueryInIdTest extends TestBase {

    @Entity("docs")
    private static class Doc {
        @Id
        private long id = 4;
    }

    @Test
    public void testInIdList() throws Exception {
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
