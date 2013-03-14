/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.net.UnknownHostException;

public class CommandResultTest extends TestCase {
    @Test
    public void testOkCommandResult() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", 1);
        assertNull(commandResult.getException());
    }

    @Test
    public void testNullErrorCode() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", 0);
        assertEquals(CommandFailureException.class, commandResult.getException().getClass());
        try {
            commandResult.throwOnError();
            fail("Should throw");
        } catch (CommandFailureException e) {
            assertEquals(commandResult, e.getCommandResult());
            assertEquals(-5, e.getCode());
        }
    }

    @Test
    public void testCommandFailure() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        final DBObject result = new BasicDBObject("ok", 0.0).append("errmsg", "no not found").append("code", 5000);
        commandResult.putAll(result);
        assertEquals(CommandFailureException.class, commandResult.getException().getClass());
        try {
            commandResult.throwOnError();
            fail("Should throw");
        } catch (CommandFailureException e) {
            assertEquals(commandResult, e.getCommandResult());
            assertEquals(5000, e.getCode());
        }
    }
}
