/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.geojson.codecs

import com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.CRS_84
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

final class NamedCoordinateReferenceSystemSpecification extends Specification {
    def registry = fromProviders([new GeoJsonCodecProvider()])
    def codec = registry.get(NamedCoordinateReferenceSystem)
    def writer = new BsonDocumentWriter(new BsonDocument())
    def context = EncoderContext.builder().build()

    def 'should round trip'() {
        when:
        codec.encode(writer, crs, context)

        then:
        writer.document == parse( "{type: 'name', properties : {name : '$crs.name'}}")

        when:
        def decodeCRS = codec.decode(new BsonDocumentReader(writer.document), DecoderContext.builder().build())

        then:
        crs == decodeCRS

        where:
        crs << [CRS_84, EPSG_4326, EPSG_4326_STRICT_WINDING]
    }

    def 'should throw when decoding invalid documents'() {
        when:
        codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)

        where:
        invalidJson << [
                '{type: "name"}',
                '{type: "name",  properties : {}}',
                '{type: "name",  properties : {type: "link", properties: {href: "http://example.com/crs/42"}}}',
        ]
    }
}
