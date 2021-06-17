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

package com.mongodb.client.model.geojson.codecs

import com.mongodb.client.model.geojson.MultiPolygon
import com.mongodb.client.model.geojson.PolygonCoordinates
import com.mongodb.client.model.geojson.Position
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class MultiPolygonCodecSpecification extends Specification {
    def registry = fromProviders([new GeoJsonCodecProvider()])
    def codec = registry.get(MultiPolygon)
    def writer = new BsonDocumentWriter(new BsonDocument())
    def context = EncoderContext.builder().build()

    def 'should round trip'() {
        given:
        def multiMultiPolygon = new MultiPolygon([new PolygonCoordinates([new Position(102.0, 2.0), new Position(103.0, 2.0),
                                                                          new Position(103.0, 3.0), new Position(102.0, 3.0),
                                                                          new Position(102.0, 2.0)]),
                                                  new PolygonCoordinates([new Position(100.0, 0.0), new Position(101.0, 0.0),
                                                                          new Position(101.0, 1.0), new Position(100.0, 1.0),
                                                                          new Position(100.0, 0.0)],
                                                                         [[new Position(100.2, 0.2), new Position(100.8, 0.2),
                                                                          new Position(100.8, 0.8), new Position(100.2, 0.8),
                                                                          new Position(100.2, 0.2)]])])

        when:
        codec.encode(writer, multiMultiPolygon, context)

        then:
        writer.document == parse('''{type: "MultiPolygon",
                coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}''')


        when:
        def decodedMultiMultiPolygon = codec.decode(new BsonDocumentReader(writer.document), DecoderContext.builder().build())

        then:
        multiMultiPolygon == decodedMultiMultiPolygon
    }

    def 'should round trip with coordinate reference system'() {
        given:
        def multiMultiPolygon = new MultiPolygon(EPSG_4326_STRICT_WINDING,
                                                 [new PolygonCoordinates([new Position(102.0, 2.0), new Position(103.0, 2.0),
                                                                          new Position(103.0, 3.0), new Position(102.0, 3.0),
                                                                          new Position(102.0, 2.0)]),
                                                  new PolygonCoordinates([new Position(100.0, 0.0), new Position(101.0, 0.0),
                                                                          new Position(101.0, 1.0), new Position(100.0, 1.0),
                                                                          new Position(100.0, 0.0)],
                                                                         [[new Position(100.2, 0.2), new Position(100.8, 0.2),
                                                                          new Position(100.8, 0.8), new Position(100.2, 0.8),
                                                                          new Position(100.2, 0.2)]])])

        when:
        codec.encode(writer, multiMultiPolygon, context)

        then:
        writer.document == parse("""{ "type": "MultiPolygon",
                coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]],
                crs: {type: 'name', properties : {name : '$EPSG_4326_STRICT_WINDING.name'}}}""")

        when:
        def decodedMultiMultiPolygon = codec.decode(new BsonDocumentReader(writer.document), DecoderContext.builder().build())

        then:
        multiMultiPolygon == decodedMultiMultiPolygon
    }

    def 'should throw when decoding invalid documents'() {
        when:
        codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)

        where:
        invalidJson << [
                '{type: "MultiPolygon"}',
                '''{coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}''',
                '''{type: "MultiPolygot",
                    coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}''',
                '{type: "MultiPolygon", coordinates: [[[40.0, 18.0], [40.0, 19.0]]]}',
                '{type: "MultiPolygon", coordinates: []}',
                '{type: "MultiPolygon", coordinates: [[]]}',
                '{type: "MultiPolygon", coordinates: [[[]]]}',
                '{type: "MultiPolygon", coordinates: [[[[]]]]}',
                '{type: "MultiPolygon", coordinates: [[[[[]]]]]}',
                "{type: 'MultiPolygon', crs : {type: 'name', properties : {name : '$EPSG_4326_STRICT_WINDING.name'}}}",
                '''{type: "MultiPolygot", crs : {type: "something"},
                    coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}''',
                '''{type: "MultiPolygot", crs : {type: "link", properties: {href: "http://example.com/crs/42"}},
                    coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}''',
                '''{type: "MultiPolygon", abc: 123,
                    coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
                              [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                              [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}'''
        ]
    }

}
