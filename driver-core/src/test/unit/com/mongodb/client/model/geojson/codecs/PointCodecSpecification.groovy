/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.client.model.geojson.codecs

import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Position
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.codecs.EncoderContext
import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class PointCodecSpecification extends Specification {
    def registry = fromProviders([new GeoJsonCodecProvider()])
    def codec = registry.get(Point)
    def writer = new BsonDocumentWriter(new BsonDocument())
    def context = EncoderContext.builder().build()

    def 'should encode'() {
        given:
        def point = new Point(new Position([40.0d, 18.0d]))

        when:
        codec.encode(writer, point, context)

        then:
        writer.document == parse('{type: \'Point\', coordinates: [40.0, 18.0]}')
    }

    def 'should encode with coordinate reference system'() {
        given:
        def point = new Point(EPSG_4326_STRICT_WINDING, new Position([40.0d, 18.0d]))

        when:
        codec.encode(writer, point, context)

        then:
        writer.document == parse('{type: \'Point\', coordinates: [40.0, 18.0], ' +
                                 "crs : {type: 'name', properties : {name : '$EPSG_4326_STRICT_WINDING.name'}}}")
    }
}
