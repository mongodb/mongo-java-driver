/*
 * Copyright 2016 MongoDB, Inc.
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
 *
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.codehaus.groovy.tools.shell.Command;
import org.junit.Test;

import java.net.UnknownHostException;

import static junit.framework.TestCase.assertEquals;

public class DuplicateKeyExceptionTest extends TestCase {

    @Test
    public void shouldGetErrorMessageFromErrField() throws UnknownHostException {
        // given
        String err = "This is an err";
        CommandResult result = new CommandResult(new ServerAddress());
        result.put("err", err);

        // when
        DuplicateKeyException e = new DuplicateKeyException(result);

        // then
        assertEquals(err, e.getErrorMessage());
    }

    @Test
    public void shouldGetErrorMessageFromErrMsgField() throws UnknownHostException {
        // given
        String err = "This is an err";
        CommandResult result = new CommandResult(new ServerAddress());
        result.put("errmsg", err);

        // when
        DuplicateKeyException e = new DuplicateKeyException(result);

        // then
        assertEquals(err, e.getErrorMessage());
    }
}
