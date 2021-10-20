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

package com.mongodb;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/uri-options/tests
public class UriOptionsTest extends AbstractConnectionStringTest {
    public UriOptionsTest(final String filename, final String description, final String input, final BsonDocument definition) {
        super(filename, description, input, definition);
    }

    @Test
    public void shouldPassAllOutcomes() {
        assumeFalse(getDefinition().getBoolean("warning", BsonBoolean.FALSE).getValue());
        assumeFalse(getDescription().equals("Arbitrary string readConcernLevel does not cause a warning"));
        // Skip because Java driver does not support the tlsAllowInvalidCertificates option
        assumeFalse(getDescription().startsWith("tlsInsecure and tlsAllowInvalidCertificates both present"));
        assumeFalse(getDescription().startsWith("tlsAllowInvalidCertificates and tlsInsecure both present"));
        // Skip because Java driver does not support srvServiceName yet
        assumeFalse(getDescription().contains("srvServiceName"));

        if (getDefinition().getBoolean("valid", BsonBoolean.TRUE).getValue()) {
            testValidOptions();
        } else {
            testInvalidUris();
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/uri-options")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        test.asDocument().getString("uri").getValue(), test.asDocument()});
            }
        }
        return data;
    }
}
