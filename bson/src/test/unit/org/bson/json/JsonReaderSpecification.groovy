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

package org.bson.json

import org.bson.BsonDocument
import org.bson.BsonHelper
import spock.lang.Specification
import spock.lang.Unroll

import static org.bson.AbstractBsonReader.State.DONE
import static org.bson.AbstractBsonReader.State.TYPE

class JsonReaderSpecification extends Specification {

    @Unroll
    def 'should skip value #value'() {
        given:
        def document = new BsonDocument('name', value)
        def reader = new JsonReader(document.toJson())
        reader.readStartDocument()
        reader.readBsonType()

        when:
        reader.skipName()
        reader.skipValue()

        then:
        reader.getState() == TYPE

        when:
        reader.readEndDocument()

        then:
        reader.getState() == DONE

        where:
        value << BsonHelper.valuesOfEveryType()
    }
}
