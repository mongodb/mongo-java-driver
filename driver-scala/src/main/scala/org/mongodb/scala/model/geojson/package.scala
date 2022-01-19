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

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

import com.mongodb.client.model.{ geojson => Jgeojson }

package object geojson {

  /**
   * A GeoJSON Coordinate Reference System (CRS).
   */
  type CoordinateReferenceSystem = Jgeojson.CoordinateReferenceSystem

  /**
   * GeoJSON coordinate reference system types.
   */
  type CoordinateReferenceSystemType = Jgeojson.CoordinateReferenceSystemType

  /**
   * GeoJSON coordinate reference system types.
   */
  object CoordinateReferenceSystemType {

    /**
     * A coordinate reference system that is specified by name
     */
    val NAME: CoordinateReferenceSystemType = Jgeojson.CoordinateReferenceSystemType.NAME

    /**
     * A coordinate reference system that is specified by a dereferenceable URI
     */
    val LINK: CoordinateReferenceSystemType = Jgeojson.CoordinateReferenceSystemType.LINK
  }

  /**
   * The GeoJSON object types.
   */
  type GeoJsonObjectType = Jgeojson.GeoJsonObjectType

  /**
   * The GeoJSON object types.
   */
  object GeoJsonObjectType {

    /**
     * A GeometryCollection
     */
    val GEOMETRY_COLLECTION: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.GEOMETRY_COLLECTION

    /**
     * A LineString
     */
    val LINE_STRING: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.LINE_STRING

    /**
     * A MultiLineString
     */
    val MULTI_LINE_STRING: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.MULTI_LINE_STRING

    /**
     * A MultiPoint
     */
    val MULTI_POINT: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.MULTI_POINT

    /**
     * A MultiPolygon
     */
    val MULTI_POLYGON: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.MULTI_POLYGON

    /**
     * A Point
     */
    val POINT: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.POINT

    /**
     * A Polygon
     */
    val POLYGON: GeoJsonObjectType = Jgeojson.GeoJsonObjectType.POLYGON
  }

  /**
   * An abstract class for representations of GeoJSON geometry objects.
   */
  type Geometry = Jgeojson.Geometry

  /**
   * A representation of a GeoJSON GeometryCollection.
   */
  type GeometryCollection = Jgeojson.GeometryCollection

  /**
   * A representation of a GeoJSON GeometryCollection.
   */
  object GeometryCollection {

    /**
     * Construct an instance with the given list of Geometry objects
     *
     * @param geometries  the list of Geometry objects
     */
    def apply(geometries: Seq[geojson.Geometry]): GeometryCollection =
      new Jgeojson.GeometryCollection(geometries.asJava)

    /**
     * Construct an instance with the given list of Geometry objects
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param geometries  the list of Geometry objects
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, geometries: Seq[Geometry]): GeometryCollection =
      new Jgeojson.GeometryCollection(coordinateReferenceSystem, geometries.asJava)
  }

  /**
   * A representation of a GeoJSON LineString.
   */
  type LineString = Jgeojson.LineString

  /**
   * A representation of a GeoJSON LineString.
   */
  object LineString {

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates  the list of Geometry objects
     * @return the new LineString
     */
    def apply(coordinates: Seq[Position]): LineString = new Jgeojson.LineString(coordinates.asJava)

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates  the list of Geometry objects
     * @return the new LineString
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinates: Seq[Position]): LineString =
      new Jgeojson.LineString(coordinateReferenceSystem, coordinates.asJava)
  }

  /**
   * A representation of a GeoJSON MultiLineString.
   */
  type MultiLineString = Jgeojson.MultiLineString

  /**
   * A representation of a GeoJSON MultiLineString.
   */
  object MultiLineString {

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates of each line
     * @return the new MultiLineString
     */
    def apply(coordinates: Seq[Position]*): MultiLineString = new MultiLineString(coordinates.map(_.asJava).asJava)

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates the coordinates of each line
     * @return the new MultiLineString
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinates: Seq[Position]*): MultiLineString =
      new Jgeojson.MultiLineString(coordinateReferenceSystem, coordinates.map(_.asJava).asJava)
  }

  /**
   * A representation of a GeoJSON MultiPoint.
   */
  type MultiPoint = Jgeojson.MultiPoint

  /**
   * A representation of a GeoJSON MultiPoint.
   */
  object MultiPoint {

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates
     * @return the new MultiPoint
     */
    def apply(coordinates: Position*): MultiPoint = new Jgeojson.MultiPoint(coordinates.asJava)

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates               the coordinates
     * @return the new MultiPoint
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinates: Position*): MultiPoint =
      new Jgeojson.MultiPoint(coordinateReferenceSystem, coordinates.asJava)
  }

  /**
   * A representation of a GeoJSON MultiPolygon.
   */
  type MultiPolygon = Jgeojson.MultiPolygon

  /**
   * A representation of a GeoJSON MultiPolygon.
   */
  object MultiPolygon {

    /**
     * Construct an instance.
     *
     * @param coordinates the coordinates
     * @return the new MultiPolygon
     */
    def apply(coordinates: PolygonCoordinates*): MultiPolygon = new Jgeojson.MultiPolygon(coordinates.asJava)

    /**
     * Construct an instance.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates the coordinates
     * @return the new MultiPolygon
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinates: PolygonCoordinates*): MultiPolygon =
      new Jgeojson.MultiPolygon(coordinateReferenceSystem, coordinates.asJava)
  }

  /**
   * A GeoJSON named Coordinate Reference System.
   */
  type NamedCoordinateReferenceSystem = Jgeojson.NamedCoordinateReferenceSystem

  /**
   * A GeoJSON named Coordinate Reference System.
   */
  object NamedCoordinateReferenceSystem {

    /**
     * The EPSG:4326 Coordinate Reference System.
     */
    val EPSG_4326: NamedCoordinateReferenceSystem = Jgeojson.NamedCoordinateReferenceSystem.EPSG_4326

    /**
     * The urn:ogc:def:crs:OGC:1.3:CRS84 Coordinate Reference System
     */
    val CRS_84: NamedCoordinateReferenceSystem = Jgeojson.NamedCoordinateReferenceSystem.CRS_84

    /**
     * A custom MongoDB EPSG:4326 Coordinate Reference System that uses a strict counter-clockwise winding order.
     *
     * [[http://docs.mongodb.org/manual/reference/operator/query/geometry/ Strict Winding]]
     */
    val EPSG_4326_STRICT_WINDING: NamedCoordinateReferenceSystem =
      Jgeojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING

    /**
     * Construct an instance
     *
     * @param name the name
     * @return the new NamedCoordinateReferenceSystem
     */
    def apply(name: String): NamedCoordinateReferenceSystem = new Jgeojson.NamedCoordinateReferenceSystem(name)
  }

  /**
   * A representation of a GeoJSON Point.
   */
  type Point = Jgeojson.Point

  /**
   * A representation of a GeoJSON Point.
   */
  object Point {

    /**
     * Construct an instance with the given coordinate.
     *
     * @param coordinate the non-null coordinate of the point
     * @return the new Point
     */
    def apply(coordinate: Position): Point = new Jgeojson.Point(coordinate)

    /**
     * Construct an instance with the given coordinate and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinate the non-null coordinate of the point
     * @return the new Point
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinate: Position): Point =
      new Jgeojson.Point(coordinateReferenceSystem, coordinate)
  }

  /**
   * A representation of a GeoJSON Polygon.
   */
  type Polygon = Jgeojson.Polygon

  /**
   * A representation of a GeoJSON Polygon.
   */
  object Polygon {

    /**
     * Construct an instance with the given coordinates.
     *
     * @param exterior the exterior ring of the polygon
     * @param holes    optional interior rings of the polygon
     * @return the new Polygon
     */
    def apply(exterior: Seq[Position], holes: Seq[Position]*): Polygon =
      new Jgeojson.Polygon(exterior.asJava, holes.map(_.asJava): _*)

    /**
     * Construct an instance with the given coordinates.
     *
     * @param coordinates the coordinates
     * @return the new Polygon
     */
    def apply(coordinates: PolygonCoordinates): Polygon = new Jgeojson.Polygon(coordinates)

    /**
     * Construct an instance with the given coordinates and coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     * @param coordinates               the coordinates
     * @return the new Polygon
     */
    def apply(coordinateReferenceSystem: CoordinateReferenceSystem, coordinates: PolygonCoordinates): Polygon =
      new Jgeojson.Polygon(coordinateReferenceSystem, coordinates)
  }

  /**
   * Coordinates for a GeoJSON Polygon.
   */
  type PolygonCoordinates = Jgeojson.PolygonCoordinates

  /**
   * Coordinates for a GeoJSON Polygon.
   */
  object PolygonCoordinates {

    /**
     * Construct an instance.
     *
     * @param exterior the exterior ring of the polygon
     * @param holes    optional interior rings of the polygon
     * @return the new PolygonCoordinates
     */
    def apply(exterior: Seq[Position], holes: Seq[Position]*): PolygonCoordinates =
      new Jgeojson.PolygonCoordinates(exterior.asJava, holes.map(_.asJava): _*)
  }

  /**
   * A representation of a GeoJSON Position.
   */
  type Position = Jgeojson.Position

  /**
   * A representation of a GeoJSON Position.
   */
  object Position {

    /**
     * Construct an instance.
     *
     * @param values the position values
     * @return the new Position
     */
    def apply(values: Double*): Position = {
      val buffer = new ArrayBuffer[java.lang.Double]
      values.foreach(buffer.append(_))
      new Jgeojson.Position(buffer.asJava)
    }
  }
}
