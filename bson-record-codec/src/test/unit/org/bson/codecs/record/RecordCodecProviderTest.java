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

package org.bson.codecs.record;

import org.bson.codecs.record.samples.TestRecord;
import org.bson.conversions.Bson;
import com.mongodb.MongoClientSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RecordCodecProviderTest {

    @Test
    public void shouldReturnNullForNonRecord() {
        var provider = new RecordCodecProvider();

        // expect
        assertNull(provider.get(String.class, Bson.DEFAULT_CODEC_REGISTRY));
    }

    @Test
    public void shouldReturnRecordCodecForRecord() {
        var provider = new RecordCodecProvider();

        // when
        var codec = provider.get(TestRecord.class, Bson.DEFAULT_CODEC_REGISTRY);

        // then
        assertTrue(codec instanceof RecordCodec);
        var recordCodec = (RecordCodec<TestRecord>) codec;
        assertEquals(TestRecord.class, recordCodec.getEncoderClass());
    }

    @Test
    public void shouldReturnRecordCodecForRecordUsingDefaultRegistry() {
        // when
        var codec = MongoClientSettings.getDefaultCodecRegistry().get(TestRecord.class);

        // then
        assertTrue(codec instanceof RecordCodec);
        var recordCodec = (RecordCodec<TestRecord>) codec;
        assertEquals(TestRecord.class, recordCodec.getEncoderClass());
    }
}
