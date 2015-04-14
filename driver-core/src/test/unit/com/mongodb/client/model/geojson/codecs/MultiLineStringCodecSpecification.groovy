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

import com.mongodb.client.model.geojson.MultiLineString
import com.mongodb.client.model.geojson.Position
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.codecs.EncoderContext
import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class MultiLineStringCodecSpecification extends Specification {
    def registry = fromProviders([new GeoJsonCodecProvider()])
    def codec = registry.get(MultiLineString)
    def writer = new BsonDocumentWriter(new BsonDocument())
    def context = EncoderContext.builder().build()

    def 'should encode'() {
        given:
        def multiLineString = new MultiLineString([[new Position([1.0d, 1.0d]), new Position([2.0d, 2.0d]), new Position([3.0d, 4.0d])],
                                                   [new Position([2.0d, 3.0d]), new Position([3.0d, 2.0d]), new Position([4.0d, 4.0d])]])

        when:
        codec.encode(writer, multiLineString, context)

        then:
        writer.document == parse('{type: \'MultiLineString\', coordinates: [' +
                                 '[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], ' +
                                 '[[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]]}')
    }

    def 'should encode with coordinate reference system'() {
        given:
        def multiLineString = new MultiLineString(EPSG_4326_STRICT_WINDING,
                                                  [[new Position([1.0d, 1.0d]), new Position([2.0d, 2.0d]), new Position([3.0d, 4.0d])],
                                                   [new Position([2.0d, 3.0d]), new Position([3.0d, 2.0d]), new Position([4.0d, 4.0d])]])

        when:
        codec.encode(writer, multiLineString, context)

        then:
        writer.document == parse('{type: \'MultiLineString\', ' +
                                 'coordinates: [' +
                                 '[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], ' +
                                 '[[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]], ' +
                                 "crs : {type: 'name', properties : {name : '$EPSG_4326_STRICT_WINDING.name'}}}")
    }
}
