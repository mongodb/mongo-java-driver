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
import org.mongodb.connection.ServerAddress;

import java.net.UnknownHostException;

import static org.junit.Assert.assertTrue;

public class WriteResultTest {
    @Test
    public void testWithCommandResult() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost", 28000), new Document("ok", 1).append("n",
                0).append("connectionId", 431), 100000L);
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        WriteResult writeResult = new WriteResult(commandResult, writeConcern);
        assertTrue(writeResult.toString().startsWith("WriteResult"));
    }

}
