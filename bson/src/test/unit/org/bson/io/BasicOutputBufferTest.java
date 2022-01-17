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

package org.bson.io;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

// for tests that are too slow to run in Groovy
public class BasicOutputBufferTest {

    @Test
    public void shouldEncodeAllCodePointsThatAreLettersOrDigits() throws IOException {
        for (int codePoint = 1; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (!Character.isLetterOrDigit(codePoint)) {
                continue;
            }
            // given
            BasicOutputBuffer bsonOutput = new BasicOutputBuffer(8);

            // when
            String str = new String(Character.toChars(codePoint));
            bsonOutput.writeCString(str);

            // then
            byte[] bytes = getBytes(bsonOutput);
            assertArrayEquals("failed with code point " + codePoint, str.getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(bytes, 0, bytes.length - 1));
        }
    }

    byte[] getBytes(final BasicOutputBuffer basicOutputBuffer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(basicOutputBuffer.getSize());

        basicOutputBuffer.pipe(baos);

        return baos.toByteArray();
    }

}
