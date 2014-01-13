/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.codecs

import org.bson.BSONWriter
import org.mongodb.Document
import org.mongodb.codecs.validators.FieldNameValidator
import org.mongodb.codecs.validators.QueryFieldNameValidator
import spock.lang.Specification
import spock.lang.Subject

class MapCodecSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    @Subject
    private MapCodec mapCodec = new MapCodec(Codecs.createDefault(), new FieldNameValidator());

    def 'should encode string to document map'() {
        given:
        Map<String, Object> map = ['myFieldName': new Document('doc', 1)];

        when:
        mapCodec.encode(bsonWriter, map);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('myFieldName');
        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('doc');
        then:
        1 * bsonWriter.writeInt32(1);
        then:
        2 * bsonWriter.writeEndDocument();
    }

    def 'should encode simple string to object map'() {
        given:
        Map<String, Object> map = ['myFieldName': 'The Field'];

        when:
        mapCodec.encode(bsonWriter, map);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('myFieldName');
        then:
        1 * bsonWriter.writeString('The Field');
        then:
        1 * bsonWriter.writeEndDocument();
    }

    def 'should not allow dots in keys when validator is collectible document validator'() {
        given:
        mapCodec = new MapCodec(Codecs.createDefault(), new FieldNameValidator());

        Map<String, Integer> mapWithInvalidFieldName = ['a.b': 1];

        when:
        mapCodec.encode(bsonWriter, mapWithInvalidFieldName);

        then:
        thrown(IllegalArgumentException)
    }

    def 'should allow dots in keys in nested maps when validator is query document validator'() {
        given:
        mapCodec = new MapCodec(Codecs.createDefault(), new QueryFieldNameValidator());
        Map<String, Object> map = ['a.b': 'The Field'];

        when:
        mapCodec.encode(bsonWriter, map);

        then:
        1 * bsonWriter.writeStartDocument();
        then:
        1 * bsonWriter.writeName('a.b');
        then:
        1 * bsonWriter.writeString('The Field');
        then:
        1 * bsonWriter.writeEndDocument();

    }

    //TODO: Trish: optimise encoding primitive types?

}
