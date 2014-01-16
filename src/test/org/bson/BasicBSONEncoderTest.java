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

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.junit.Test;

import java.util.regex.Pattern;

public class BasicBSONEncoderTest {

    @Test(expected =  BSONException.class)
    public void nullCharacterInCStringShouldThrowException() {
        BasicBSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        encoder.writeCString("hell\u0000world");
    }

    @Test(expected = BSONException.class)
    public void nullCharacterInKeyShouldThrowException() {
        BasicBSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        encoder.putString("ke\u0000y", "helloWorld");
    }

    @Test(expected = BSONException.class)
    public void nullCharacterInRegexStringShouldThrowException() {
        BasicBSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        encoder._putObjectField("key", Pattern.compile("hello\u0000World"));
    }

    @Test
    public void nullCharacterInStringShouldNotThrowException() {
        BasicBSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        encoder.putString("key", "hell\u0000world");
    }
}
