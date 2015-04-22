/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.geojson.codecs

import com.mongodb.client.model.geojson.GeometryCollection
import com.mongodb.client.model.geojson.LineString
import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Position
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.codecs.EncoderContext
import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class GeometryCollectionCodecSpecification extends Specification {
    def registry = fromProviders([new GeoJsonCodecProvider()])
    def codec = registry.get(GeometryCollection)
    def writer = new BsonDocumentWriter(new BsonDocument())
    def context = EncoderContext.builder().build()

    def 'should encode'() {
        given:
        def geometryCollection = new GeometryCollection([new Point(new Position(100d, 0d)),
                                                         new LineString([new Position(101d, 0d), new Position(102d, 1d)])])

        when:
        codec.encode(writer, geometryCollection, context)

        then:
        writer.document == parse('{ type: "GeometryCollection",' +
                                 '    geometries: [' +
                                 '      { type: "Point", coordinates: [100.0, 0.0]},' +
                                 '      { type: "LineString", coordinates: [ [101.0, 0.0], [102.0, 1.0] ]}' +
                                 '    ]}')
    }

    def 'should encode with coordinate reference system'() {
        given:
        def geometryCollection = new GeometryCollection(EPSG_4326_STRICT_WINDING,
                                                        [new Point(new Position(100d, 0d)),
                                                         new LineString([new Position(101d, 0d), new Position(102d, 1d)])])

        when:
        codec.encode(writer, geometryCollection, context)

        then:
        writer.document == parse('{ type: "GeometryCollection",' +
                                 '    geometries: [' +
                                 '      { type: "Point", coordinates: [100.0, 0.0]},' +
                                 '      { type: "LineString", coordinates: [ [101.0, 0.0], [102.0, 1.0] ]}, ' +
                                 '    ]' +
                                 "   crs : {type: 'name', properties : {name : '$EPSG_4326_STRICT_WINDING.name'}}}")
    }
}
