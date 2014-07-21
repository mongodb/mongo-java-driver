/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.client.QueryBuilder;
import org.junit.Test;

import static com.mongodb.client.QueryBuilder.query;
import static com.mongodb.client.QueryOperators.TYPE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueryBuilderTest {
    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithDocumentVarargsOperands() {
        QueryBuilder queryBuilder = QueryBuilder.query().or(new Document("name", "first"), new Document("age", 43));

        String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithTwoStreamedDocumentOperands() {
        QueryBuilder queryBuilder = QueryBuilder.query().or(new Document("name", "first"))
                                                .or(new Document("age", 43));

        String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/or/
     */
    @Test
    public void shouldCreateValidBSONDocumentForOrWithTwoStreamedQueryBuilderOperands() {
        QueryBuilder queryBuilder = QueryBuilder.query().or(QueryBuilder.query("name").is("first"))
                                                .or(QueryBuilder.query("age").is(43));

        String expectedQuery = "{ \"$or\" : [{ \"name\" : \"first\" }, { \"age\" : 43 }] }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForValue() {
        QueryBuilder queryBuilder = QueryBuilder.query("name").is("first");

        String expectedQuery = "{ \"name\" : \"first\" }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForQueryBuilderValue() {
        QueryBuilder queryBuilder = QueryBuilder.query("numericValue").is(query(TYPE).is(16));

        String expectedQuery = "{ \"numericValue\" : { \"$type\" : 16 } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForText() {
        QueryBuilder queryBuilder = QueryBuilder.query().text("dolor");

        String expectedQuery = "{ \"$text\" : { \"$search\" : \"dolor\" } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateValidBSONDocumentToTestForTextWithLanguage() {
        QueryBuilder queryBuilder = QueryBuilder.query().text("dolor", "latin");

        String expectedQuery = "{ \"$text\" : { \"$search\" : \"dolor\", \"$language\" : \"latin\" } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateCorrectNearQueryWhenMaxDistanceIsSpecified() {
        QueryBuilder queryBuilder = QueryBuilder.query("loc").near(45.0, 45.0, 0.5);

        String expectedQuery = "{ \"loc\" : { \"$near\" : [45.0, 45.0], \"$maxDistance\" : 0.5 } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    @Test
    public void shouldCreateCorrectNearSphereQueryWhenMaxDistanceIsSpecified() {
        QueryBuilder queryBuilder = QueryBuilder.query("loc").nearSphere(45.0, 45.0, 0.5);

        String expectedQuery = "{ \"loc\" : { \"$nearSphere\" : [45.0, 45.0], \"$maxDistance\" : 0.5 } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }
}
