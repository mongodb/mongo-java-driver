/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public abstract class AbstractMainTransactionsTest extends AbstractUnifiedTest {
    public AbstractMainTransactionsTest(final String filename, final String description, final String databaseName,
                                        final String collectionName, final BsonArray data,
                                        final BsonDocument definition, final boolean skipTest) {
        super(filename, description, databaseName, collectionName, data, definition, skipTest, true);
        // Tests of distinct in transactions can fail with a StaleDbVersion error.  See
        // https://github.com/mongodb/specifications/blob/master/source/transactions/tests/README.rst
        // #why-do-tests-that-run-distinct-sometimes-fail-with-staledbversion
        // But in the Java driver test suite the tests are only failing on 4.2 sharded clusters, so rather than implement the workaround
        // suggested in the specification, we just skip the tests on 4.2.
        assumeFalse((
                description.equals("only first distinct includes readConcern")
                        || description.equals("distinct ignores collection readConcern")
                        || description.equals("distinct"))
                && isSharded()
                && serverVersionLessThan(4, 4));
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/transactions")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);

            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getString("collection_name",
                                new BsonString(file.getName().substring(0, file.getName().lastIndexOf(".")))).getValue(),
                        testDocument.getArray("data"), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }
}
