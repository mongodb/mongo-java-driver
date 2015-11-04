/*
 * Copyright 2015 MongoDB, Inc.
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
 *
 */

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// BSON tests powered by language-agnostic JSON-based tests included in test resources
@RunWith(Parameterized.class)
public class GenericBsonTest {

    private final BsonDocument definition;

    public GenericBsonTest(final String description, final BsonDocument definition) {
        this.definition = definition;
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue curValue : definition.getArray("documents")) {
            BsonDocument curDocument = curValue.asDocument();
            try {
                validateDecoding(curDocument);
                validateEncoding(curDocument);
            } catch (BsonSerializationException e) {
                // should not throw unless the test case has an error string
                if (!curDocument.containsKey("error")) {
                    fail(curDocument.getString("error").getValue() + ": " + e.getMessage());
                }
            } catch (RuntimeException e) {
                fail("Threw RuntimeException instead of BsonSerializationException: " + e.toString());
            }
        }
    }

    private String validateDecoding(final BsonDocument curDocument) {
        BsonDocument expectedDecodedDocument = curDocument.getDocument("decoded", null);
        String encoded = curDocument.getString("encoded").getValue();

        if (encoded != null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(DatatypeConverter.parseHexBinary(encoded));
            BsonDocument actualDecodedDocument = new BsonDocumentCodec().decode(new BsonBinaryReader(byteBuffer),
                                                                                DecoderContext.builder().build());

            if (byteBuffer.hasRemaining()) {
                fail("Should have consumed all bytes, but " + byteBuffer.remaining() + " still remain in the buffer");
            } else if (expectedDecodedDocument == null) {
                fail("Decoding of '" + encoded + "' should have failed with error '"
                     + curDocument.getString("error").getValue()
                     + "' but succeeded with " + actualDecodedDocument.toJson());
            } else {
                assertEquals("Failed to decode to expected document", expectedDecodedDocument, actualDecodedDocument);
            }
        }
        return encoded;
    }

    private void validateEncoding(final BsonDocument curDocument) {
        BsonDocument expectedDecodedDocument = curDocument.getDocument("decoded", null);
        if (expectedDecodedDocument != null && !curDocument.getBoolean("decodeOnly", BsonBoolean.FALSE).getValue()) {
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), expectedDecodedDocument, EncoderContext.builder().build());
            assertEquals("Failed to properly encode", curDocument.getString("encoded").getValue(),
                         DatatypeConverter.printHexBinary(buffer.toByteArray()));
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/bson")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{testDocument.getString("description").getValue(), testDocument});
        }
        return data;
    }
}
