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

package org.mongodb.scala.model

import java.lang.reflect.Modifier._

import org.mongodb.scala.BaseSpec
import org.mongodb.scala.model.geojson.NamedCoordinateReferenceSystem._
import org.mongodb.scala.model.geojson._

import scala.collection.JavaConverters._

class GeoJsonSpec extends BaseSpec {

  it should "have the same methods as the wrapped CoordinateReferenceSystemType" in {
    val wrapped = classOf[geojson.CoordinateReferenceSystemType].getDeclaredFields
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = CoordinateReferenceSystemType.getClass.getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet

    local should equal(wrapped)
  }

  it should "have the same methods as the wrapped GeoJsonObjectType" in {
    val wrapped = classOf[geojson.GeoJsonObjectType].getDeclaredFields
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = GeoJsonObjectType.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "have the same CoordinateReferenceSystemType" in {
    CoordinateReferenceSystemType.LINK should equal(geojson.CoordinateReferenceSystemType.LINK)
    CoordinateReferenceSystemType.NAME should equal(geojson.CoordinateReferenceSystemType.NAME)
  }

  it should "have the same GeoJsonObjectType" in {
    GeoJsonObjectType.GEOMETRY_COLLECTION should equal(geojson.GeoJsonObjectType.GEOMETRY_COLLECTION)
    GeoJsonObjectType.LINE_STRING should equal(geojson.GeoJsonObjectType.LINE_STRING)
    GeoJsonObjectType.MULTI_LINE_STRING should equal(geojson.GeoJsonObjectType.MULTI_LINE_STRING)
    GeoJsonObjectType.MULTI_POINT should equal(geojson.GeoJsonObjectType.MULTI_POINT)
    GeoJsonObjectType.POINT should equal(geojson.GeoJsonObjectType.POINT)
    GeoJsonObjectType.POLYGON should equal(geojson.GeoJsonObjectType.POLYGON)
  }

  it should "create the same GeometryCollection" in {
    GeometryCollection(Seq(Point(Position(1, 2)))) should equal(
      new geojson.GeometryCollection(Seq(Point(Position(1, 2))).asInstanceOf[List[Geometry]].asJava)
    )

    GeometryCollection(EPSG_4326, Seq(Point(Position(1, 2)))) should equal(
      new geojson.GeometryCollection(EPSG_4326, Seq(Point(Position(1, 2))).asInstanceOf[List[Geometry]].asJava)
    )
  }

  it should "create the same LineString" in {
    LineString(Seq(Position(1, 2), Position(2, 4))) should equal(
      new geojson.LineString(Seq(Position(1, 2), Position(2, 4)).asJava)
    )
    LineString(EPSG_4326_STRICT_WINDING, Seq(Position(1, 2), Position(2, 4))) should equal(
      new geojson.LineString(EPSG_4326_STRICT_WINDING, Seq(Position(1, 2), Position(2, 4)).asJava)
    )
  }

  it should "create the same MultiLineString" in {
    MultiLineString(Seq(Position(1, 2))) should equal(
      new geojson.MultiLineString(Seq(Seq(Position(1, 2)).asJava).asJava)
    )
    MultiLineString(EPSG_4326, Seq(Position(1, 2))) should equal(
      new geojson.MultiLineString(EPSG_4326, Seq(Seq(Position(1, 2)).asJava).asJava)
    )
  }

  it should "create the same MultiPoint" in {
    MultiPoint(Position(1, 2)) should equal(new geojson.MultiPoint(Seq(Position(1, 2)).asJava))
    MultiPoint(CRS_84, Position(1, 2)) should equal(new geojson.MultiPoint(CRS_84, Seq(Position(1, 2)).asJava))
  }

  it should "create the same Point" in {
    Point(Position(1, 2)) should equal(new geojson.Point(Position(1, 2)))
    Point(CRS_84, Position(1, 2)) should equal(new geojson.Point(CRS_84, Position(1, 2)))
  }

  it should "create the same MultiPolygon" in {
    val exterior = Seq(Position(10, 20), Position(10, 40), Position(20, 40), Position(10, 20))
    val interior = Seq(Position(15, 16), Position(15, 18), Position(16, 18), Position(15, 16))

    MultiPolygon(PolygonCoordinates(exterior)) should equal(
      new geojson.MultiPolygon(Seq(PolygonCoordinates(exterior)).asJava)
    )

    MultiPolygon(PolygonCoordinates(exterior, interior)) should equal(
      new geojson.MultiPolygon(Seq(PolygonCoordinates(exterior, interior)).asJava)
    )

    MultiPolygon(CRS_84, PolygonCoordinates(exterior)) should equal(
      new geojson.MultiPolygon(CRS_84, Seq(PolygonCoordinates(exterior)).asJava)
    )
  }

  it should "create the same Polygon" in {
    val exterior = Seq(Position(10, 20), Position(10, 40), Position(20, 40), Position(10, 20))

    Polygon(PolygonCoordinates(exterior)) should equal(new geojson.Polygon(PolygonCoordinates(exterior)))
    Polygon(CRS_84, PolygonCoordinates(exterior)) should equal(
      new geojson.Polygon(CRS_84, PolygonCoordinates(exterior))
    )
  }

  it should "create a NamedCoordinateReferenceSystem from a string" in {
    val coordinateRefSystem = NamedCoordinateReferenceSystem("EPSG:4326")

    coordinateRefSystem should equal(EPSG_4326)
  }
}
