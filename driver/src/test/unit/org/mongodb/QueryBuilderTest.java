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

import org.junit.Test;
import org.mongodb.geojson.*;

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

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/queryr/near/
     */
    @Test
    public void shouldCreateCorrectNearGeoJsonQueryWithoutMaxDistance() throws Exception {
        GeoJsonPoint point = new GeoJsonPoint( new GeoJson2DCoordinates(45.0, 0.5));
        QueryBuilder queryBuilder = QueryBuilder.query("loc").near(point);

        String expectedQuery = "{ \"loc\" : { \"$near\" : { \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [45.0, 0.5] } } } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/queryr/near/
     */
    @Test
    public void shouldCreateCorrectNearGeoJsonQueryWhenMaxDistanceIsSpecified() throws Exception {
        GeoJsonPoint point = new GeoJsonPoint( new GeoJson2DCoordinates(45.0, 0.5));
        QueryBuilder queryBuilder = QueryBuilder.query("loc").near(point, 0.5);

        String expectedQuery = "{ \"loc\" : { \"$near\" : { \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [45.0, 0.5] }, \"$maxDistance\" : 0.5 } } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/query/geoWithin/
     */
    @Test
    public void shouldCreateCorrectGeoWithinQueryWithSimplePolygon() throws Exception {
        GeoJsonPolygon polygon = new GeoJsonPolygon(
                GeoJson.position(-108.62131299999987, 45.00027699999998),
                GeoJson.position(-104.05769699999995,44.997380000000135),
                GeoJson.position(-104.053249, 41.00140600000009),
                GeoJson.position(-111.04672299999999, 40.99795899999998),
                GeoJson.position(-111.05519899999989, 45.001321000000075),
                GeoJson.position(-108.62131299999987, 45.00027699999998) );
        QueryBuilder queryBuilder = QueryBuilder.query("loc").geoWithin(polygon);

        String expectedQuery = "{ \"loc\" : { \"$geoWithin\" : { \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : "
                                    +"[[[-108.62131299999987, 45.00027699999998], [-104.05769699999995, 44.997380000000135], "
                                    +"[-104.053249, 41.00140600000009], [-111.04672299999999, 40.99795899999998], [-111.05519899999989, "
                                    +"45.001321000000075], [-108.62131299999987, 45.00027699999998]]] } } } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/query/geoIntersects/
     */
    @Test
    public void shouldCreateCorrectGeoIntersectsQueryWithVariousObjects() throws Exception {
        GeoJsonPolygon polygon = new GeoJsonPolygon(
                GeoJson.position(-108.62131299999987, 45.00027699999998),
                GeoJson.position(-104.05769699999995,44.997380000000135),
                GeoJson.position(-104.053249, 41.00140600000009),
                GeoJson.position(-111.04672299999999, 40.99795899999998),
                GeoJson.position(-111.05519899999989, 45.001321000000075),
                GeoJson.position(-108.62131299999987, 45.00027699999998) );
        QueryBuilder queryBuilder = QueryBuilder.query("loc").geoIntersects(polygon);

        String expectedQuery = "{ \"loc\" : { \"$geoIntersects\" : { \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : "
                                    +"[[[-108.62131299999987, 45.00027699999998], [-104.05769699999995, 44.997380000000135], "
                                    +"[-104.053249, 41.00140600000009], [-111.04672299999999, 40.99795899999998], [-111.05519899999989, "
                                    +"45.001321000000075], [-108.62131299999987, 45.00027699999998]]] } } } }";

        assertThat(queryBuilder.toDocument().toString(), is(expectedQuery));
    }

    /**
     * @link http://docs.mongodb.org/manual/reference/operator/geoIntersects/
     */
    @Test(expected = QueryBuilder.QueryBuilderException.class)
    public void shouldRaiseAnExceptionWhenIntersectsQueryIsUsedWithInvalidTypes() throws Exception {
        GeoJsonMultiPoint multiPoint = new GeoJsonMultiPoint(
                new GeoJson2DCoordinates(100.0, 0.0),
                new GeoJson2DCoordinates(101.0, 1.0)
        );
        QueryBuilder queryBuilder = QueryBuilder.query("loc").geoIntersects(multiPoint);
        queryBuilder.toString(); // to use the variable
    }

}
