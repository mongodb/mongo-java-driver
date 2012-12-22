/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.bson.types.Document;
import org.junit.Test;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.operation.MongoInsert;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class GetLastErrorTest extends MongoClientTestBase {
    @Test
    public void testDuplicateKeyException() {
        Document doc = new Document("_id", 1);
        collection.insert(new MongoInsert<Document>(doc));
        try {
            collection.insert(new MongoInsert<Document>(doc));
            fail("should throw exception");
        } catch (MongoDuplicateKeyException e) {
            assertThat(e.getCommandResult().getErrorCode(), is(11000));
        }
    }
}
