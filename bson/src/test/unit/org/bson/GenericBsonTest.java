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

package org.bson;

import org.bson.json.JsonMode;
import org.bson.json.JsonParseException;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.bson.BsonDocument.parse;
import static org.bson.BsonHelper.decodeToDocument;
import static org.bson.BsonHelper.encodeToHex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

// BSON tests powered by language-agnostic JSON-based tests included in test resources
public class GenericBsonTest {

    private static final List<String> IGNORED_PARSE_ERRORS = Arrays.asList(
            "Bad $binary (type is number, not string)", // for backwards compat, JsonReader supports number for binary type
            "Bad $date (number, not string or hash)",   // for backwards compat, JsonReader supports numbers for $date
            "Bad DBRef (ref is number, not string)",    // JsonReader knows nothing of DBRef so these are not parse errors
            "Bad DBRef (db is number, not string)");

    enum TestCaseType {
        VALID,
        DECODE_ERROR,
        PARSE_ERROR
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("data")
    public void shouldPassAllOutcomes(@SuppressWarnings("unused") final String description,
            final BsonDocument testDefinition, final BsonDocument testCase, final TestCaseType testCaseType) {
        switch (testCaseType) {
            case VALID:
                runValid(testCase);
                break;
            case DECODE_ERROR:
                runDecodeError(testCase);
                break;
            case PARSE_ERROR:
                runParseError(testDefinition, testCase);
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported test case type %s", testCaseType));
        }
    }

    private void runValid(final BsonDocument testCase) {
        String description = testCase.getString("description").getValue();
        String canonicalBsonHex = testCase.getString("canonical_bson").getValue().toUpperCase();
        String degenerateBsonHex = testCase.getString("degenerate_bson", new BsonString("")).getValue().toUpperCase();
        String canonicalJson = replaceUnicodeEscapes(testCase.getString("canonical_extjson").getValue());
        String relaxedJson = replaceUnicodeEscapes(testCase.getString("relaxed_extjson", new BsonString("")).getValue());
        String degenerateJson = replaceUnicodeEscapes(testCase.getString("degenerate_extjson", new BsonString("")).getValue());
        boolean lossy = testCase.getBoolean("lossy", new BsonBoolean(false)).getValue();

        BsonDocument decodedDocument = decodeToDocument(canonicalBsonHex, description);

        // native_to_bson( bson_to_native(cB) ) = cB
        assertEquals(canonicalBsonHex, encodeToHex(decodedDocument),
                format("Failed to create expected BSON for document with description '%s'", description));

        JsonWriterSettings canonicalJsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
        JsonWriterSettings relaxedJsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

        if (!canonicalJson.isEmpty()) {
            // native_to_canonical_extended_json( bson_to_native(cB) ) = cEJ
            assertEquals(stripWhiteSpace(canonicalJson), stripWhiteSpace(decodedDocument.toJson(canonicalJsonWriterSettings)),
                    format("Failed to create expected canonical JSON for document with description '%s'", description));

            // native_to_canonical_extended_json( json_to_native(cEJ) ) = cEJ
            BsonDocument parsedCanonicalJsonDocument = parse(canonicalJson);
            assertEquals(stripWhiteSpace(canonicalJson), stripWhiteSpace(parsedCanonicalJsonDocument.toJson(canonicalJsonWriterSettings)),
                    "Failed to create expected canonical JSON from parsing canonical JSON");

            if (!lossy) {
                // native_to_bson( json_to_native(cEJ) ) = cB
                assertEquals(canonicalBsonHex, encodeToHex(parsedCanonicalJsonDocument),
                        "Failed to create expected canonical BSON from parsing canonical JSON");
            }
        }

        if (!relaxedJson.isEmpty()) {
            // native_to_relaxed_extended_json( bson_to_native(cB) ) = rEJ
            assertEquals(stripWhiteSpace(relaxedJson), stripWhiteSpace(decodedDocument.toJson(relaxedJsonWriterSettings)),
                    format("Failed to create expected relaxed JSON for document with description '%s'", description));

            // native_to_relaxed_extended_json( json_to_native(rEJ) ) = rEJ
            assertEquals(stripWhiteSpace(relaxedJson), stripWhiteSpace(parse(relaxedJson).toJson(relaxedJsonWriterSettings)),
                    "Failed to create expected relaxed JSON from parsing relaxed JSON");
        }

        if (!degenerateJson.isEmpty()) {
            // native_to_bson( json_to_native(dEJ) ) = cB
            assertEquals(canonicalBsonHex, encodeToHex(parse(degenerateJson)),
                    "Failed to create expected canonical BSON from parsing canonical JSON");
        }

        if (!degenerateBsonHex.isEmpty()) {
            BsonDocument decodedDegenerateDocument = decodeToDocument(degenerateBsonHex, description);
            // native_to_bson( bson_to_native(dB) ) = cB
            assertEquals(canonicalBsonHex, encodeToHex(decodedDegenerateDocument),
                    format("Failed to create expected canonical BSON from degenerate BSON for document with description '%s'",
                            description));
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
                    writer.write("\\u" + new String(codePointString));
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

    private void runDecodeError(final BsonDocument testCase) {
        try {
            String description = testCase.getString("description").getValue();
            throwIfValueIsStringContainingReplacementCharacter(testCase, description);
            fail(format("Should have failed parsing for subject with description '%s'", description));
        } catch (BsonSerializationException e) {
            // all good
        }
    }

    private void runParseError(final BsonDocument testDefinition, final BsonDocument testCase) {
        String description = testCase.getString("description").getValue();

        assumeFalse(IGNORED_PARSE_ERRORS.contains(description));

        String str = testCase.getString("string").getValue();

        String testDefinitionDescription = testDefinition.getString("description").getValue();
        if (testDefinitionDescription.startsWith("Decimal128")) {
            try {
                Decimal128.parse(str);
                fail(format("Should fail to parse '" + str + "' with description '%s'", description + "'"));
            } catch (NumberFormatException e) {
                // all good
            }
        } else if (testDefinitionDescription.startsWith("Top-level") || testDefinitionDescription.startsWith("Binary type")) {
            try {
                BsonDocument document = parse(str);
                encodeToHex(document);
                fail("Should fail to parse JSON '" + str + "' with description '" + description + "'");
            } catch (JsonParseException e) {
                // all good
            } catch (BsonInvalidOperationException e) {
                if (!description.equals("Bad $code with $scope (scope is number, not doc)")) {
                    fail("Should throw JsonParseException for '" + str + "' with description '" + description + "'");
                }
                // all good
            } catch (BsonSerializationException e) {
                if (isTestOfNullByteInCString(description)) {
                    assertTrue(e.getMessage().contains("is not valid because it contains a null character"));
                } else {
                    fail("Unexpected BsonSerializationException");
                }
            }
        } else {
            fail("Unrecognized test definition description: " + testDefinitionDescription);
        }
    }

    private boolean isTestOfNullByteInCString(final String description) {
        return description.startsWith("Null byte");
    }

    // Working around the fact that the Java driver doesn't report an error for invalid UTF-8, but rather replaces the invalid
    // sequence with the replacement character
    private void throwIfValueIsStringContainingReplacementCharacter(final BsonDocument testCase, final String description) {
        BsonDocument decodedDocument = decodeToDocument(testCase.getString("bson").getValue(), description);
        BsonValue value = decodedDocument.get(decodedDocument.getFirstKey());

        String decodedString;
        if (value.isString()) {
            decodedString = value.asString().getValue();
        } else if (value.isDBPointer()) {
            decodedString = value.asDBPointer().getNamespace();
        } else if (value.isJavaScript()) {
            decodedString = value.asJavaScript().getCode();
        } else if (value.isJavaScriptWithScope()) {
            decodedString = value.asJavaScriptWithScope().getCode();
        } else if (value.isSymbol()) {
            decodedString = value.asSymbol().getSymbol();
        } else {
            throw new UnsupportedOperationException("Unsupported test for BSON type " + value.getBsonType());
        }

        if (decodedString.contains(StandardCharsets.UTF_8.newDecoder().replacement())) {
            throw new BsonSerializationException("String contains replacement character");
        }
    }


    private static Stream<Arguments> data() throws URISyntaxException, IOException {
        List<Arguments> data = new ArrayList<>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/bson")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue curValue : testDocument.getArray("valid", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(Arguments.of(
                        createTestCaseDescription(testDocument, testCaseDocument, "valid"), testDocument, testCaseDocument,
                        TestCaseType.VALID));
            }

            for (BsonValue curValue : testDocument.getArray("decodeErrors", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(Arguments.of(
                        createTestCaseDescription(testDocument, testCaseDocument, "decodeError"), testDocument, testCaseDocument,
                        TestCaseType.DECODE_ERROR));
            }
            for (BsonValue curValue : testDocument.getArray("parseErrors", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(Arguments.of(createTestCaseDescription(testDocument, testCaseDocument, "parseError"), testDocument,
                        testCaseDocument, TestCaseType.PARSE_ERROR));
            }
        }
        return data.stream();
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
