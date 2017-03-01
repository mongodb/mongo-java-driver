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
 *
 */

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// BSON tests powered by language-agnostic JSON-based tests included in test resources
@RunWith(Parameterized.class)
public class GenericBsonTest {

    enum TestCaseType {
        VALID,
        PARSE_ERROR
    }

    private final Set<String> testsToSkip = new HashSet<String>(Arrays.asList(
            "DBpointer with opposite key order",                             // JsonReader does not support out of order keys
            "DBpointer with extra keys"                                      // JsonReader does not support extra keys
            ));

    private final BsonDocument testDefinition;
    private final BsonDocument testCase;
    private final TestCaseType testCaseType;

    public GenericBsonTest(final String description, final BsonDocument testDefinition, final BsonDocument testCase,
                           final TestCaseType testCaseType) {
        this.testDefinition = testDefinition;
        this.testCase = testCase;
        this.testCaseType = testCaseType;
    }

    @Test
    public void shouldPassAllOutcomes() {
        assumeTrue(!testsToSkip.contains(testCase.getString("description").getValue()));
        switch (testCaseType) {
            case VALID:
                runValid();
                break;
            case PARSE_ERROR:
                runDecodeError();
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported test case type %s", testCaseType));
        }
    }

    private void runValid() {
        String bsonHex = testCase.getString("bson").getValue().toUpperCase();
        String json = replaceUnicodeEscapes(testCase.getString("extjson", new BsonString("")).getValue());
        String canonicalJson = replaceUnicodeEscapes(testCase.getString("canonical_extjson", new BsonString(json)).getValue());
        String canonicalBsonHex = testCase.getString("canonical_bson", new BsonString(bsonHex)).getValue().toUpperCase();
        String description = testCase.getString("description").getValue();
        boolean lossy = testCase.getBoolean("lossy", new BsonBoolean(false)).getValue();

        BsonDocument decodedDocument = decodeToDocument(bsonHex, description);

        // B -> B
        assertEquals(format("Failed to create expected BSON for document with description '%s'", description),
                canonicalBsonHex, encodeToHex(decodedDocument));

        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

        // B -> E
        if (!canonicalJson.isEmpty()) {
            assertEquals(format("Failed to create expected JSON for document with description '%s'", description),
                    stripWhiteSpace(canonicalJson), stripWhiteSpace(decodedDocument.toJson(jsonWriterSettings)));
        }

        if (!canonicalBsonHex.equals(bsonHex)) {
            BsonDocument decodedCanonicalDocument = decodeToDocument(canonicalBsonHex, description);
            // B -> B
            assertEquals(format("Failed to create expected BSON for canonical document with description '%s'", description),
                    canonicalBsonHex, encodeToHex(decodedCanonicalDocument));
            // B -> E
            assertEquals(format("Failed to create expected JSON for canonical document with description '%s'", description),
                    stripWhiteSpace(canonicalJson), stripWhiteSpace(decodedCanonicalDocument.toJson(jsonWriterSettings)));
        }

        if (!json.isEmpty()) {
            BsonDocument parsedDocument = BsonDocument.parse(json);
            // E -> E
            assertEquals(format("Failed to parse expected JSON for document with description '%s'", description),
                    stripWhiteSpace(canonicalJson), stripWhiteSpace(parsedDocument.toJson(jsonWriterSettings)));

            if (!lossy) {
                // E -> B
                assertEquals(format("Failed to create expected BsonDocument for parsed canonical JSON document with description '%s'",
                        description), decodedDocument, parsedDocument);
                assertEquals(format("Failed to create expected BSON for parsed JSON document with description '%s'", description),
                        canonicalBsonHex, encodeToHex(parsedDocument));

            }
            if (!canonicalJson.equals(json)) {
                BsonDocument parsedCanonicalDocument = BsonDocument.parse(canonicalJson);
                // E -> E
                assertEquals(format("Failed to create expected JSON for parsed canonical JSON document with description '%s'",
                        description), stripWhiteSpace(canonicalJson), stripWhiteSpace(parsedCanonicalDocument.toJson(jsonWriterSettings)));
                if (!lossy) {
                    // E -> B
                    assertEquals(format("Failed to create expected BsonDocument for parsed canonical JSON document "
                                                + "with description '%s'", description), decodedDocument, parsedCanonicalDocument);
                    assertEquals(format("Failed to create expected BSON for parsed canonical JSON document with description '%s'",
                            description), bsonHex, encodeToHex(parsedDocument));
                }
            }
        }
    }

    // The corpus escapes all non-ascii characters, but JSONWriter does not.  This method converts the Unicode escape sequence into its
    // regular UTF encoding in order to match the JSONWriter behavior.
    private String replaceUnicodeEscapes(final String json) {
        try {
            StringReader reader = new StringReader(json);
            StringWriter writer = new StringWriter();
            int cur;
            while ((cur = reader.read()) != -1) {
                char curChar = (char) cur;
                if (curChar != '\\') {
                    writer.write(curChar);
                    continue;
                }

                char nextChar = (char) reader.read();
                if (nextChar != 'u') {
                    writer.write(curChar);
                    writer.write(nextChar);
                    continue;
                }
                char[] codePointString = new char[4];
                reader.read(codePointString);
                char escapedChar = (char) Integer.parseInt(new String(codePointString), 16);
                if (shouldEscapeCharacter(escapedChar)) {
                    writer.write("\\u0000");
                } else {
                    writer.write(escapedChar);
                }

            }
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException("impossible");
        }
    }

    // copied from JsonWriter...
    private boolean shouldEscapeCharacter(final char escapedChar) {
        switch (Character.getType(escapedChar)) {
            case Character.UPPERCASE_LETTER:
            case Character.LOWERCASE_LETTER:
            case Character.TITLECASE_LETTER:
            case Character.OTHER_LETTER:
            case Character.DECIMAL_DIGIT_NUMBER:
            case Character.LETTER_NUMBER:
            case Character.OTHER_NUMBER:
            case Character.SPACE_SEPARATOR:
            case Character.CONNECTOR_PUNCTUATION:
            case Character.DASH_PUNCTUATION:
            case Character.START_PUNCTUATION:
            case Character.END_PUNCTUATION:
            case Character.INITIAL_QUOTE_PUNCTUATION:
            case Character.FINAL_QUOTE_PUNCTUATION:
            case Character.OTHER_PUNCTUATION:
            case Character.MATH_SYMBOL:
            case Character.CURRENCY_SYMBOL:
            case Character.MODIFIER_SYMBOL:
            case Character.OTHER_SYMBOL:
                return false;
            default:
                return true;
        }
    }

    private BsonDocument decodeToDocument(final String subjectHex, final String description) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(DatatypeConverter.parseHexBinary(subjectHex));
        BsonDocument actualDecodedDocument = new BsonDocumentCodec().decode(new BsonBinaryReader(byteBuffer),
                DecoderContext.builder().build());

        if (byteBuffer.hasRemaining()) {
            throw new BsonSerializationException(format("Should have consumed all bytes, but " + byteBuffer.remaining()
                                                                + " still remain in the buffer for document with description ",
                    description));
        }
        return actualDecodedDocument;
    }

    private String encodeToHex(final BsonDocument decodedDocument) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), decodedDocument, EncoderContext.builder().build());
        return DatatypeConverter.printHexBinary(outputBuffer.toByteArray());
    }

    private void runDecodeError() {
        try {
            String description = testCase.getString("description").getValue();
            throwIfValueIsStringContainingReplacementCharacter(description);
            fail(format("Should have failed parsing for subject with description '%s'", description));
        } catch (BsonSerializationException e) {
            // all good
        }
    }

    // TODO: Working around the fact that the Java driver doesn't report an error for invalid UTF-8, but rather replaces the invalid
    // sequence with the replacement character
    private void throwIfValueIsStringContainingReplacementCharacter(final String description) {
        BsonDocument decodedDocument = decodeToDocument(testCase.getString("bson").getValue(), description);
        String testKey = decodedDocument.keySet().iterator().next();

        if (decodedDocument.containsKey(testKey)) {
            String decodedString = null;
            if (decodedDocument.get(testKey).isString()) {
                decodedString = decodedDocument.getString(testKey).getValue();
            }
            if (decodedDocument.get(testKey).isDBPointer()) {
                decodedString = decodedDocument.get(testKey).asDBPointer().getNamespace();
            }
            if (decodedString != null && decodedString.contains(Charset.forName("UTF-8").newDecoder().replacement())) {
                throw new BsonSerializationException("String contains replacement character");
            }

        }
    }


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/bson")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue curValue : testDocument.getArray("valid", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(new Object[]{createTestCaseDescription(testDocument, testCaseDocument, "valid"), testDocument, testCaseDocument,
                        TestCaseType.VALID});
            }

            for (BsonValue curValue : testDocument.getArray("decodeErrors", new BsonArray())) {
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
