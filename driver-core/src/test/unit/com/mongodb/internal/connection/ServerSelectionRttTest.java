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

package com.mongodb.internal.connection;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

// See https://github.com/mongodb/specifications/tree/master/source/server-selection/tests
@RunWith(Parameterized.class)
public class ServerSelectionRttTest {
    private final BsonDocument definition;

    public ServerSelectionRttTest(final String description, final BsonDocument definition) {
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        RoundTripTimeSampler subject = new RoundTripTimeSampler();

        BsonValue current = definition.get("avg_rtt_ms");
        if (current.isNumber()) {
            subject.addSample(current.asNumber().longValue());
        }

        subject.addSample(definition.getNumber("new_rtt_ms").longValue());
        long expected = definition.getNumber("new_avg_rtt").asNumber().longValue();

        assertEquals(subject.getAverage(), expected);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        for (BsonDocument testDocument : JsonPoweredTestHelper.getTestDocuments("/server-selection/rtt")) {
            data.add(new Object[]{testDocument.getString("fileName").getValue(), testDocument});
        }
        return data;
    }

}
