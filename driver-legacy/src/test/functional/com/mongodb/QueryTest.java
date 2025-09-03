/*
 * Copyright 2008-present MongoDB, Inc.
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

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class QueryTest extends DatabaseTestCase {

    @Test
    public void shouldBeAbleToUseOldQueryBuilderWithNewFilterMethod() {
        // given
        collection.insert(new BasicDBObject("name", "Bob"));

        //when
        DBObject filter = QueryBuilder.start("name").is("Bob").get();
        DBCursor dbCursor = collection.find(filter);

        //then
        assertThat(dbCursor.next().get("name").toString(), is("Bob"));
    }

    @Test
    @Ignore
    public void shouldBeAbleToQueryWithJSONString() {
        //        JSON.parse(jsonString);
        //        collection.find(JSON.parse(jsonString))
    }

}
