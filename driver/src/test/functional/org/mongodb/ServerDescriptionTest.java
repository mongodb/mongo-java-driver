/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.junit.Test;
import org.mongodb.command.MongoCommand;
import org.mongodb.connection.ServerDescription;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ServerDescriptionTest extends DatabaseTestCase {

    private final MongoDatabase adminDatabase = Fixture.getMongoClient().getDatabase("admin");
    private final Document isMasterCommandDocument = new Document("ismaster", 1);
    private final MongoCommand command = new MongoCommand(isMasterCommandDocument);

    @Test
    public void testIsPrimary() {
        ServerDescription result =
        new ServerDescription(adminDatabase.executeCommand(command));
        assertTrue(result.isPrimary());
    }

    @Test
    public void testIsSecondary() {
        ServerDescription result =
        new ServerDescription(adminDatabase.executeCommand(command));
        assertFalse(result.isSecondary());
    }
}
