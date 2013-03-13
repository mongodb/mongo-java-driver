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

package org.mongodb;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.QueryBuilder.query;
import static org.mongodb.QueryOperators.TYPE;

public class QueryBuilderTest {
    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithDocumentVarargsOperands() {
        final QueryBuilder queryBuilder = QueryBuilder.query().or(new Document("name", "first"), new Document("age", 43));

        final String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithTwoStreamedDocumentOperands() {
        final QueryBuilder queryBuilder = QueryBuilder.query().or(new Document("name", "first"))
                                                              .or(new Document("age", 43));

        final String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithTwoStreamedQueryBuilderOperands() {
        final QueryBuilder queryBuilder = QueryBuilder.query().or(QueryBuilder.query("name").is("first"))
                                                              .or(QueryBuilder.query("age").is(43));

        final String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForValue() {
        final QueryBuilder queryBuilder = QueryBuilder.query("name").is("first");

        final String expectedQuery = "{ \"name\" : \"first\" }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForQueryBuilderValue() {
        final QueryBuilder queryBuilder = QueryBuilder.query("numericValue").is(query(TYPE).is(16));

        final String expectedQuery = "{ \"numericValue\" : { \"$type\" : 16 } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

}
