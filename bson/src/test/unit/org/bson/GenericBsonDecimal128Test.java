/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// BSON tests powered by language-agnostic JSON-based tests included in test resources
@RunWith(Parameterized.class)
public class GenericBsonDecimal128Test {

    enum TestCaseType {
        VALID,
        PARSE_ERROR
    }

    private final BsonDocument testDefinition;
    private final BsonDocument testCase;
    private final TestCaseType testCaseType;

    public GenericBsonDecimal128Test(final String description, final BsonDocument testDefinition, final BsonDocument testCase,
                                     final TestCaseType testCaseType) {
        this.testDefinition = testDefinition;
        this.testCase = testCase;
        this.testCaseType = testCaseType;
    }

    @Test
    public void shouldPassAllOutcomes() {
        switch (testCaseType) {
            case VALID:
                runValid();
                break;
            case PARSE_ERROR:
                runParseError();
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported test case type %s", testCaseType));
        }
    }

    private void runValid() {
        String bsonHex = testCase.getString("bson").getValue().toUpperCase();
        String json = testCase.getString("extjson").getValue();
        String canonicalJson = testCase.getString("canonical_extjson", new BsonString(json)).getValue();
        String description = testCase.getString("description").getValue();
        boolean lossy = testCase.getBoolean("lossy", new BsonBoolean(false)).getValue();

        BsonDocument decodedDocument = decodeToDocument(bsonHex, description);

        // B -> B
        assertEquals(format("Failed to create expected BSON for document with description '%s'", description),
                bsonHex, encodeToHex(decodedDocument));

        // B -> E
        assertEquals(format("Failed to create expected JSON for document with description '%s'", description),
                stripWhiteSpace(canonicalJson), stripWhiteSpace(decodedDocument.toJson()));

        // E -> E
        BsonDocument parsedDocument = BsonDocument.parse(json);
        assertEquals(format("Failed to parse expected JSON for document with description '%s'", description),
                stripWhiteSpace(canonicalJson), stripWhiteSpace(parsedDocument.toJson()));

        if (!lossy) {
            // E -> B
            assertEquals(format("Failed to create expected BsonDocument for parsed canonical JSON document with description '%s'",
                    description), decodedDocument, parsedDocument);
            assertEquals(format("Failed to create expected BSON for parsed JSON document with description '%s'", description),
                    bsonHex, encodeToHex(parsedDocument));

        }

        if (!canonicalJson.equals(json)) {
            BsonDocument parsedCanonicalDocument = BsonDocument.parse(canonicalJson);
            // E -> E
            assertEquals(format("Failed to create expected JSON for parsed canonical JSON document with description '%s'", description),
                    stripWhiteSpace(canonicalJson), stripWhiteSpace(parsedCanonicalDocument.toJson()));
            if (!lossy) {
                // E -> B
                assertEquals(format("Failed to create expected BsonDocument for parsed canonical JSON document with description '%s'",
                        description), decodedDocument, parsedCanonicalDocument);
                assertEquals(format("Failed to create expected BSON for parsed canonical JSON document with description '%s'", description),
                        bsonHex, encodeToHex(parsedDocument));
            }
        }
    }

    private BsonDocument decodeToDocument(final String subjectHex, final String description) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(DatatypeConverter.parseHexBinary(subjectHex));
        BsonDocument actualDecodedDocument = new BsonDocumentCodec().decode(new BsonBinaryReader(byteBuffer),
                DecoderContext.builder().build());

        if (byteBuffer.hasRemaining()) {
            fail(format("Should have consumed all bytes, but " + byteBuffer.remaining() + " still remain in the buffer "
                                + "for document with description ", description));
        }
        return actualDecodedDocument;
    }

    private String encodeToHex(final BsonDocument decodedDocument) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), decodedDocument, EncoderContext.builder().build());
        return DatatypeConverter.printHexBinary(outputBuffer.toByteArray());
    }

    private void runParseError() {
        try {
            String description = testCase.getString("description").getValue();
            Decimal128.parse(testCase.getString("string").getValue());
            fail(format("Should have failed parsing for subject with description '%s'", description));
        } catch (NumberFormatException e) {
            // all good
        } catch (IllegalArgumentException e) {
            // all good
        }
    }


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/decimal128")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue curValue : testDocument.getArray("valid", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(new Object[]{createTestCaseDescription(testDocument, testCaseDocument, "valid"), testDocument, testCaseDocument,
                        TestCaseType.VALID});
            }

            for (BsonValue curValue : testDocument.getArray("parseErrors", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(new Object[]{createTestCaseDescription(testDocument, testCaseDocument, "parseError"), testDocument,
                        testCaseDocument, TestCaseType.PARSE_ERROR});
            }
        }
        return data;
    }

    private static String createTestCaseDescription(final BsonDocument testDocument, final BsonDocument testCaseDocument,
                                                    final String testCaseType) {
        return testDocument.getString("description").getValue()
                       + "[" + testCaseType + "]"
                       + ": " + testCaseDocument.getString("description").getValue();
    }

    private String stripWhiteSpace(final String json) {
        return json.replace(" ", "");
    }
}
