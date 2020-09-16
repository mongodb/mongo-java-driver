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

package org.bson.codecs.pojo;

import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.records.GenericHolderRecord;
import org.bson.codecs.pojo.entities.records.NestedRecordsAndPojos;
import org.bson.codecs.pojo.entities.records.PojoContainsGenericRecord;
import org.bson.codecs.pojo.entities.records.PojoContainsRecord;
import org.bson.codecs.pojo.entities.records.RecordContainsGenericPojo;
import org.bson.codecs.pojo.entities.records.RecordContainsPojo;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class RecordPojoIntegrationTest extends PojoTestCase {
    public static CodecRegistry registry = fromProviders(
            new BsonValueCodecProvider(),
            new ValueCodecProvider(),
            RecordCodecProvider.builder().automatic(true).build(),
            PojoCodecProvider.builder().automatic(true).build());

    private final static String RECORD_CONTAINS_POJO_JSON = "{'stringField': 'stringField', 'simpleModel': " + SIMPLE_MODEL_JSON + "}";
    private final static String POJO_CONTAINS_RECORD_JSON = "{'stringField': 'stringField', 'simpleRecord': " + SIMPLE_MODEL_JSON + "}";

    @Test
    public void RecordContainsPojo() {
        roundTrip(registry, new RecordContainsPojo("stringField", getSimpleModel()), RECORD_CONTAINS_POJO_JSON);
    }

    @Test
    public void PojoContainsRecord() {
        roundTrip(registry, new PojoContainsRecord("stringField", RecordRoundTripTest.getSimpleRecord()), POJO_CONTAINS_RECORD_JSON);
    }

    @Test
    public void RecordContainsGenericPojo() {
        roundTrip(registry, new RecordContainsGenericPojo("stringField", new GenericHolderModel<>("stringGeneric", 1L)),
                "{'stringField': 'stringField', 'genericField': {'myGenericField': 'stringGeneric', 'myLongField': {'$numberLong': '1'}}}");
    }

    @Test
    public void PojoContainsGenericRecord() {
        roundTrip(registry, new PojoContainsGenericRecord("stringField", new GenericHolderRecord<>("myGenericField", 1L)),
                "{'stringField': 'stringField', 'genericRecord': {'myGenericField': 'myGenericField', 'myLongField': {'$numberLong': '1'}}}");
    }

    @Test
    public void RecordContainsNestedPojosAndRecords() {
        roundTrip(registry, new NestedRecordsAndPojos("stringField", getSimpleModel(), RecordRoundTripTest.getSimpleRecord(),
                        new PojoContainsRecord("stringField", RecordRoundTripTest.getSimpleRecord()), new RecordContainsPojo("stringField", getSimpleModel())),
                "{'stringField': 'stringField', 'simpleModel': " + SIMPLE_MODEL_JSON + "'simpleRecord':" + SIMPLE_MODEL_JSON + "'pojoContainer':" + POJO_CONTAINS_RECORD_JSON +
                "'recordContainer':" + RECORD_CONTAINS_POJO_JSON + "}");
    }

}
