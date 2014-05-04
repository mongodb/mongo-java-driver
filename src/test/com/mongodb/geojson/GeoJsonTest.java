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


package com.mongodb.geojson;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by tgrall on 5/3/14.
 */
public class GeoJsonTest {

    @Test
    public void geoJsonTypeTest() {


        DBObject expected = null;

        Coordinates mongodbOffice = new Coordinates(-73.98783, 40.757486 );


        // Test point
        Point point = new Point(-73.98783, 40.757486);
        expected = BasicDBObjectBuilder.start()
                        .add("type", "Point" )
                        .add("coordinates", new Double[]{-73.98783, 40.757486})
                        .get();
        assertEquals(point.getLongitude(), mongodbOffice.getLongitude(), 0);
        assertEquals( point.getLatitude() , mongodbOffice.getLatitude(),0);
        assertEquals( expected, point.get() );


        // Test Polygon : simple region
        Coordinates point1 = new Coordinates( 1.4392181, 43.6288169 );
        Coordinates point2 = new Coordinates( 1.4404412, 43.6273491 );
        Coordinates point3 = new Coordinates( 1.4371582, 43.631892 );

        expected = BasicDBObjectBuilder.start()
                        .add("type", "Polygon")
                        .add("coordinates", new Double[][][] { {
                                                                {1.4392181, 43.6288169} ,
                                                                {1.4404412, 43.6273491} ,
                                                                {1.4371582, 43.631892} ,
                                                                {1.4392181, 43.6288169}
                                                               }
                                                             } )
                        .get();



        // TODO : test multi polygons
//        List<Coordinates> exteriorRing = new ArrayList<Coordinates>();
//        exteriorRing.add( point1 );
//        exteriorRing.add( point2 );
//        exteriorRing.add( point3 );
//        exteriorRing.add( point1 );
//
//        Polygon polygon = new Polygon( exteriorRing );
//        assertEquals(  expected , polygon.get() );
//
//
//
//        // Test Polygon with 2 zones see http://docs.mongodb.org/manual/core/2dsphere/#polygon
//        Coordinates point4 = new Coordinates( 2,2 );
//        Coordinates point5 = new Coordinates( 3,3 );
//        Coordinates point6 = new Coordinates( 4,2 );
//
//
//        expected = BasicDBObjectBuilder.start()
//                .add("Polygon", new Double[][][] { {{0.0,0.0} , {3.0,6.0} , {6.0,1.0} , {0.0,0.0} } , { {2.0, 2.0} , { 3.0 , 3.0 } , { 4.0 , 2.0 } , {2.0, 2.0}  }  })
//                .get();
//
//
//        List<Coordinates> interiorRing = new ArrayList<Coordinates>();
//        interiorRing.add( point4 );
//        interiorRing.add( point5 );
//        interiorRing.add( point6 );
//        interiorRing.add( point4 );
//        polygon = new Polygon( exteriorRing,interiorRing );
//
//        assertEquals( expected, polygon.get()  );



        // LineString
        LineString lineString = new LineString();
        lineString.addPoint( new Point(1.419497, 43.651485)  );
        lineString.addPoint( new Point(1.4360423, 43.6211749)  );

        expected = BasicDBObjectBuilder.start()
                .add("type", "LineString")
                .add("coordinates", new Double[][] {
                        {1.419497, 43.651485} ,
                        {1.4360423, 43.6211749}
                } )
                .get();

        assertEquals( lineString.get() , expected );


        // MultiPoint
        MultiPoint multiPoint = new MultiPoint();
        multiPoint.addPoint( new Point(1.419497, 43.651485)  );
        multiPoint.addPoint( new Point(1.4360423, 43.6211749)  );
        multiPoint.addPoint( new Point(1.4371582, 43.631892)  );

        expected = BasicDBObjectBuilder.start()
                .add("type", "MultiPoint")
                .add("coordinates", new Double[][] {
                        {1.419497, 43.651485} ,
                        {1.4360423, 43.6211749},
                        {1.4371582, 43.631892}
                } )
                .get();

        assertEquals(expected , multiPoint.get());


        // MultiLineString
        LineString lineStringOne = new LineString();
        lineStringOne.addPoint( new Point(1.419497, 43.651485)  );
        lineStringOne.addPoint( new Point(1.4360423, 43.6211749)  );

        LineString lineStringTwo = new LineString();
        lineStringTwo.addPoint( new Point(1.429497, 43.671485)  );
        lineStringTwo.addPoint( new Point(1.4460423, 43.6811749)  );

        MultiLineString multiLineString = new MultiLineString();
        multiLineString.addLineString(lineStringOne);
        multiLineString.addLineString(lineStringTwo);

        expected = BasicDBObjectBuilder.start()
                .add("type", "MultiLineString")
                .add("coordinates", new Double[][][] {
                        { {1.419497, 43.651485 } , { 1.4360423, 43.6211749 } },
                        { {1.429497, 43.671485 } , { 1.4460423, 43.6811749 } }
                } )
                .get();

        assertEquals(expected , multiLineString.get());


        // Multi Polygon
        MultiPolygon multiPolygon = new MultiPolygon();

        Coordinates polyOnePoint1 = new Coordinates( 1.4392181, 43.6288169 );
        Coordinates polyOnePoint2 = new Coordinates( 1.4404412, 43.6273491 );
        Coordinates polyOnePoint3 = new Coordinates( 1.4371582, 43.631892 );

        Coordinates polyTwoPoint1 = new Coordinates( 1.419497, 43.651485 );
        Coordinates polyTwoPoint2 = new Coordinates( 1.4360423, 43.6211749 );
        Coordinates polyTwoPoint3 = new Coordinates( 1.419497, 43.651485);

        Polygon polyOne = new Polygon( polyOnePoint1 , polyOnePoint2 , polyOnePoint3, polyOnePoint1 );
        Polygon polyTwo = new Polygon( polyTwoPoint1 , polyTwoPoint2 , polyTwoPoint3, polyTwoPoint1 );

        multiPolygon.addPolygon(polyOne);
        multiPolygon.addPolygon(polyTwo);

        expected = BasicDBObjectBuilder.start()
                .add("type", "MultiPolygon")
                .add("coordinates", new Double[][][][] {
                        {{ {1.4392181, 43.6288169 } , {1.4404412, 43.6273491} , {1.4371582, 43.631892} , {1.4392181, 43.6288169 } }},
                        {{ {1.419497, 43.651485} , {1.4360423, 43.6211749} , {1.419497, 43.651485} , {1.419497, 43.651485}}}
                } )
                .get();

        assertEquals(expected , multiPolygon.get());


    }



}
