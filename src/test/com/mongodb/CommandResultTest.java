/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.util.TestCase;
import org.junit.Test;

import java.net.UnknownHostException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommandResultTest extends TestCase {

    @Test
    public void shouldBeOkWhenOkFieldIsTrue() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", true);
        assertTrue(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWithNoOkField() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWhenOkFieldIsFalse() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", false);
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldBeOkWhenOkFieldIsOne() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", 1.0);
        assertTrue(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWhenOkFieldIsZero() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", 0.0);
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldNotHaveExceptionWhenOkIsTrue() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", true);
        assertNull(commandResult.getException());
    }

    @Test
    public void okShouldThrowWhenOkFieldTypeIsNotBooleanOrNumber() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress("localhost"));
        commandResult.put("ok", "1");
        assertFalse(commandResult.ok());
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void shouldThrowDuplicateKeyWhenResponseHasADuplicateKeyErrorCode() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress());
        commandResult.put("ok", 1);
        commandResult.put("err", "E11000 duplicate key error index 1");
        commandResult.put("code", 11000);
        commandResult.throwOnError();
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void shouldThrowDuplicateKeyWhenErrObjectsHasADuplicateKeyErrorCode() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new ServerAddress());
        commandResult.put("ok", 1);
        commandResult.put("err", "E11000 duplicate key error index 1");
        commandResult.put("errObjects", asList(new BasicDBObject("ok", 1)
                                               .append("err", "E11000 duplicate key error index 1")
                                               .append("code", 11000),
                                               new BasicDBObject("ok", 1)
                                               .append("err", "E11000 duplicate key error index 2")
                                               .append("code", 11000)));
        commandResult.throwOnError();
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
