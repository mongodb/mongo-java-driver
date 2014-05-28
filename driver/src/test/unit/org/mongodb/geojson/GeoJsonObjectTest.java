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

package org.mongodb.geojson;


import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 *  This class is used to test all the GeoJson helper classes, to validate that the Document created is a projer
 *  GeoJSON document.
 */
public class GeoJsonObjectTest {

    @Test
    public void testGeoJsonCoordinates() throws Exception {

        GeoJsonCoordinates coordinates = null;
        List<Double> expected = new ArrayList<Double>();
        expected.add(-73.98783);
        expected.add(40.757486);

        coordinates = new GeoJson2DCoordinates(-73.98783, 40.757486);
        assertEquals(expected , coordinates.get());
        expected.add(55.5);

        coordinates = new GeoJson3DCoordinates(-73.98783, 40.757486  , 55.5);
        assertEquals(expected , coordinates.get());

    }

    @Test
    public void testGeoJsonPoint() throws Exception {
        GeoJsonPoint point = new GeoJsonPoint(new GeoJson2DCoordinates(100.0, 0.0));
        String expected = "{ \"type\" : \"Point\", \"coordinates\" : [100.0, 0.0] }";
        assertEquals(100.0 ,  point.getLongitude(), 0);
        assertEquals(0 ,  point.getLatitude(), 0);
        assertEquals(expected , point.toString());
    }


    @Test
    public void testGeoJsonPolygon() throws Exception {
        // Polygon create with multiple points
        GeoJsonPolygon polygon = new GeoJsonPolygon(
                new GeoJson2DCoordinates(100.0, 0.0),
                new GeoJson2DCoordinates(101.0, 0.0),
                new GeoJson2DCoordinates(101.0, 1.0),
                new GeoJson2DCoordinates(100.0, 0.0));
        String expected = "{ \"type\" : \"Polygon\", \"coordinates\" : [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 0.0]]] }";

        assertEquals(expected, polygon.toString());

        // Polygon created from a list
        List<GeoJsonCoordinates> exteriorRing = new ArrayList<GeoJsonCoordinates>();
        exteriorRing.add(GeoJson.position(100.0, 0.0));
        exteriorRing.add(GeoJson.position(101.0, 0.0));
        exteriorRing.add(GeoJson.position(101.0, 1.0));
        exteriorRing.add(GeoJson.position(100.0, 0.0));
        polygon = new GeoJsonPolygon(exteriorRing);

        assertEquals(expected, polygon.toString());

        // Polygon with holes
        exteriorRing = new ArrayList<GeoJsonCoordinates>();
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 0.0));
        exteriorRing.add(new GeoJson2DCoordinates(101.0, 0.0));
        exteriorRing.add(new GeoJson2DCoordinates(101.0, 1.0));
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 1.0));
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 0.0));

        List<GeoJsonCoordinates> interiorRing = new ArrayList<GeoJsonCoordinates>();
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.2));
        interiorRing.add(new GeoJson2DCoordinates(100.8, 0.2));
        interiorRing.add(new GeoJson2DCoordinates(100.8, 0.8));
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.8));
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.2));

        polygon = new GeoJsonPolygon(exteriorRing, interiorRing);

        expected = "{ \"type\" : \"Polygon\", \"coordinates\" : "
                          + "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]], "
                          + "[[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]] }";
        assertEquals(expected, polygon.toString());
    }

    @Test
    public void testGeoJsonLineString() {
        GeoJsonLineString lineString = new GeoJsonLineString(
                    new GeoJson2DCoordinates(100.0, 0.0),
                    new GeoJson2DCoordinates(101.0, 1.0)
                 );

        String expected = "{ \"type\" : \"LineString\", \"coordinates\" : [[100.0, 0.0], [101.0, 1.0]] }";
        assertEquals(expected, lineString.toString());
    }

    @Test
    public void testGeoJsonMultiPoint() throws Exception {
        GeoJsonMultiPoint multiPoint = new GeoJsonMultiPoint(
                new GeoJson2DCoordinates(100.0, 0.0),
                new GeoJson2DCoordinates(101.0, 1.0)
     );
        String expected = "{ \"type\" : \"MultiPoint\", \"coordinates\" : [[100.0, 0.0], [101.0, 1.0]] }";
        assertEquals(expected, multiPoint.toString());
    }

    @Test
    public void testGeoJsonMultiLineString() throws Exception {
        List<GeoJsonCoordinates> line1 = new ArrayList<GeoJsonCoordinates>();
        line1.add(GeoJson.position(100.0, 0.0));
        line1.add(GeoJson.position(101.0, 1.0));

        List<GeoJsonCoordinates> line2 = new ArrayList<GeoJsonCoordinates>();
        line2.add(GeoJson.position(102.0, 2.0));
        line2.add(GeoJson.position(103.0, 3.0));

        List<List<GeoJsonCoordinates>> allLines = new ArrayList<List<GeoJsonCoordinates>>();
        allLines.add(line1);
        allLines.add(line2);

        GeoJsonMultiLineString multiLineString = new GeoJsonMultiLineString(allLines);
        String expected = "{ \"type\" : \"MultiLineString\", \"coordinates\" : "
                        + "[[[100.0, 0.0], [101.0, 1.0]], [[102.0, 2.0], [103.0, 3.0]]] }";
        assertEquals(expected, multiLineString.toString());
    }


    @Test
    public void testMultiPolygon() throws Exception {
        // Polygon with holes
        List<GeoJsonCoordinates> exteriorRing = new ArrayList<GeoJsonCoordinates>();
        exteriorRing = new ArrayList<GeoJsonCoordinates>();
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 0.0));
        exteriorRing.add(new GeoJson2DCoordinates(101.0, 0.0));
        exteriorRing.add(new GeoJson2DCoordinates(101.0, 1.0));
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 1.0));
        exteriorRing.add(new GeoJson2DCoordinates(100.0, 0.0));

        List<GeoJsonCoordinates> interiorRing = new ArrayList<GeoJsonCoordinates>();
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.2));
        interiorRing.add(new GeoJson2DCoordinates(100.8, 0.2));
        interiorRing.add(new GeoJson2DCoordinates(100.8, 0.8));
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.8));
        interiorRing.add(new GeoJson2DCoordinates(100.2, 0.2));

        GeoJsonPolygon polygon1 = new GeoJsonPolygon(
                new GeoJson2DCoordinates(102.0, 2.0),
                new GeoJson2DCoordinates(103.0, 2.0),
                new GeoJson2DCoordinates(103.0, 3.0),
                new GeoJson2DCoordinates(102.0, 3.0),
                new GeoJson2DCoordinates(102.0, 2.0));

        GeoJsonPolygon polygon2 = new GeoJsonPolygon(exteriorRing);
        GeoJsonMultiPolygon multiPolygon1 = new GeoJsonMultiPolygon(polygon1, polygon2);
        String expected = "{ \"type\" : \"MultiPolygon\", \"coordinates\" : "
                        + "[[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]], "
                        + "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]] }";
        assertEquals(expected, multiPolygon1.toString());

        GeoJsonPolygon polygon3 = new GeoJsonPolygon(exteriorRing, interiorRing);
        GeoJsonMultiPolygon multiPolygon2 = new GeoJsonMultiPolygon(polygon1, polygon3);
        expected = "{ \"type\" : \"MultiPolygon\", \"coordinates\" : "
                               + "[[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],"
                               + " [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],"
                               + " [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]] }";
        assertEquals(expected, multiPolygon2.toString());
    }


    @Test
    public void testGeoJsonGeometryCollectionTest() throws Exception {
        GeoJsonPoint point = new GeoJsonPoint(new GeoJson2DCoordinates(100.0, 0.0));
        GeoJsonLineString lineString =  new GeoJsonLineString(
                                                        new GeoJson2DCoordinates(101.0, 0.0),
                                                        new GeoJson2DCoordinates(102.0, 1.0));
        GeoJsonGeometryCollection geoJsonGeometryCollection = new GeoJsonGeometryCollection(point, lineString);

        String expected = "{ \"type\" : \"GeometryCollection\", \"geometries\" : "
                        + "[{ \"type\" : \"Point\", \"coordinates\" : [100.0, 0.0] }, "
                        + "{ \"type\" : \"LineString\", \"coordinates\" : [[101.0, 0.0], [102.0, 1.0]] }] }";

        assertEquals(expected, geoJsonGeometryCollection.toString());
    }
}