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

package org.bson.vector;

import org.bson.BinaryVector;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Float32BinaryVector;
import org.bson.PackedBitBinaryVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;
import util.JsonPoweredTestHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.bson.BsonHelper.decodeToDocument;
import static org.bson.BsonHelper.encodeToHex;
import static org.bson.internal.vector.BinaryVectorHelper.determineVectorDType;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/tree/master/source/bson-binary-vector/tests">JSON-based tests that included in test resources</a>.
 */
class BinaryVectorGenericBsonTest {

    private static final List<String> TEST_NAMES_TO_IGNORE = asList(
            //It is impossible to provide float inputs for INT8.
            "Underflow Vector PACKED_BIT",
            //It is impossible to provide float inputs for PACKED_BIT in the API.
            "Vector with float values PACKED_BIT",
            //It is impossible to provide float inputs for INT8.
            "Overflow Vector PACKED_BIT");

    @ParameterizedTest(name = "{0}")
    @MethodSource("data")
    void shouldPassAllOutcomes(@SuppressWarnings("unused") final String description,
                               final BsonDocument testDefinition, final BsonDocument testCase) {
        String testDescription = testCase.get("description").asString().getValue();
        assumeFalse(TEST_NAMES_TO_IGNORE.contains(testDescription));

        String testKey = testDefinition.getString("test_key").getValue();
        boolean isValidVector = testCase.getBoolean("valid").getValue();
        if (isValidVector) {
            runValidTestCase(testKey, testCase);
        } else {
            runInvalidTestCase(testDescription, testCase);
        }
    }

    private static void runInvalidTestCase(final String testDescription, final BsonDocument testCase) {
        if (testCase.containsKey("vector")) {
            assertValidationException(assertThrows(RuntimeException.class, () -> runInvalidTestCaseVector(testCase)));
        }

        // TODO JAVA-5848 in 6.0.0 "Padding specified with no vector data PACKED_BIT" will throw an error (currently logs a warning).
        if (testCase.containsKey("canonical_bson") && !testDescription.equals("Padding specified with no vector data PACKED_BIT")) {
            assertValidationException(assertThrows(RuntimeException.class, () -> runInvalidTestCaseCanonicalBson(testCase)));
        }
    }

    private static void runInvalidTestCaseVector(final BsonDocument testCase) {
        BsonArray arrayVector = testCase.getArray("vector");
        byte dtypeByte = Byte.decode(testCase.getString("dtype_hex").getValue());
        BinaryVector.DataType expectedDType = determineVectorDType(dtypeByte);
        switch (expectedDType) {
            case INT8:
                if (testCase.containsKey("padding")) {
                    throw new IllegalArgumentException("Int8 is not supported with padding");
                }
                byte[] expectedVectorData = toByteArray(arrayVector);
                BinaryVector.int8Vector(expectedVectorData);
                break;
            case PACKED_BIT:
                byte expectedPadding = (byte) testCase.getInt32("padding", new BsonInt32(0)).getValue();
                byte[] expectedVectorPackedBitData = toByteArray(arrayVector);
                BinaryVector.packedBitVector(expectedVectorPackedBitData, expectedPadding);
                break;
            case FLOAT32:
                if (testCase.containsKey("padding")) {
                    throw new IllegalArgumentException("Float32 is not supported with padding");
                }
                float[] expectedFloatVector = toFloatArray(arrayVector);
                BinaryVector.floatVector(expectedFloatVector);
                break;
            default:
                throw new AssertionFailedError("Unsupported vector data type: " + expectedDType);
        }
    }

    private static void runInvalidTestCaseCanonicalBson(final BsonDocument testCase) {
        String description = testCase.getString("description").getValue();
        byte dtypeByte = Byte.decode(testCase.getString("dtype_hex").getValue());
        String canonicalBsonHex = testCase.getString("canonical_bson").getValue().toUpperCase();
        byte[] bytes = decodeToDocument(canonicalBsonHex, description).getBinary("vector").getData();

        BinaryVector.DataType expectedDType = determineVectorDType(dtypeByte);

        switch (expectedDType) {
            case INT8:
                if (testCase.containsKey("padding")) {
                    throw new IllegalArgumentException("Int8 is not supported with padding");
                }
                BinaryVector.int8Vector(bytes);
                break;
            case PACKED_BIT:
                byte expectedPadding = (byte) testCase.getInt32("padding", new BsonInt32(0)).getValue();
                BinaryVector.packedBitVector(bytes, expectedPadding);
                break;
            case FLOAT32:
                throw new IllegalArgumentException("Float32 is not supported");
            default:
                throw new AssertionFailedError("Unsupported vector data type: " + expectedDType);
        }

    }

    private static void runValidTestCase(final String testKey, final BsonDocument testCase) {
        String description = testCase.getString("description").getValue();
        byte dtypeByte = Byte.decode(testCase.getString("dtype_hex").getValue());

        byte expectedPadding = (byte) testCase.getInt32("padding").getValue();
        BinaryVector.DataType expectedDType = determineVectorDType(dtypeByte);
        String expectedCanonicalBsonHex = testCase.getString("canonical_bson").getValue().toUpperCase();

        BsonArray arrayVector = testCase.getArray("vector");
        BsonDocument actualDecodedDocument = decodeToDocument(expectedCanonicalBsonHex, description);
        BinaryVector actualVector = actualDecodedDocument.getBinary("vector").asVector();

        switch (expectedDType) {
            case INT8:
                byte[] expectedVectorData = toByteArray(arrayVector);
                byte[] actualVectorData = actualVector.asInt8Vector().getData();
                assertVectorDecoding(
                        expectedVectorData,
                        expectedDType,
                        actualVectorData,
                        actualVector);

                assertThatVectorCreationResultsInCorrectBinary(BinaryVector.int8Vector(expectedVectorData),
                        testKey,
                        actualDecodedDocument,
                        expectedCanonicalBsonHex,
                        description);
                break;
            case PACKED_BIT:
                PackedBitBinaryVector actualPackedBitVector = actualVector.asPackedBitVector();
                byte[] expectedVectorPackedBitData = toByteArray(arrayVector);
                assertVectorDecoding(
                        expectedVectorPackedBitData,
                        expectedDType, expectedPadding,
                        actualPackedBitVector);

                assertThatVectorCreationResultsInCorrectBinary(
                        BinaryVector.packedBitVector(expectedVectorPackedBitData, expectedPadding),
                        testKey,
                        actualDecodedDocument,
                        expectedCanonicalBsonHex,
                        description);
                break;
            case FLOAT32:
                Float32BinaryVector actualFloat32Vector = actualVector.asFloat32Vector();
                float[] expectedFloatVector = toFloatArray(arrayVector);
                assertVectorDecoding(
                        expectedFloatVector,
                        expectedDType,
                        actualFloat32Vector);
                assertThatVectorCreationResultsInCorrectBinary(
                        BinaryVector.floatVector(expectedFloatVector),
                        testKey,
                        actualDecodedDocument,
                        expectedCanonicalBsonHex,
                        description);
                break;
            default:
                throw new IllegalArgumentException("Unsupported vector data type: " + expectedDType);
        }
    }

    private static void assertValidationException(final RuntimeException runtimeException) {
        assertTrue(runtimeException instanceof IllegalArgumentException || runtimeException instanceof IllegalStateException);
    }

    private static void assertThatVectorCreationResultsInCorrectBinary(final BinaryVector expectedVectorData,
                                                                       final String testKey,
                                                                       final BsonDocument actualDecodedDocument,
                                                                       final String expectedCanonicalBsonHex,
                                                                       final String description) {
        BsonDocument documentToEncode = new BsonDocument(testKey, new BsonBinary(expectedVectorData));
        assertEquals(documentToEncode, actualDecodedDocument);
        assertEquals(expectedCanonicalBsonHex, encodeToHex(documentToEncode),
                format("Failed to create expected BSON for document with description '%s'", description));
    }

    private static void assertVectorDecoding(final byte[] expectedVectorData,
                                      final BinaryVector.DataType expectedDType,
                                      final byte[] actualVectorData,
                                      final BinaryVector actualVector) {
        Assertions.assertArrayEquals(actualVectorData, expectedVectorData,
                () -> "Actual: " + Arrays.toString(actualVectorData) + " !=  Expected:" + Arrays.toString(expectedVectorData));
        assertEquals(expectedDType, actualVector.getDataType());
    }

    private static void assertVectorDecoding(final byte[] expectedVectorData,
                                      final BinaryVector.DataType expectedDType,
                                      final byte expectedPadding,
                                      final PackedBitBinaryVector actualVector) {
        byte[] actualVectorData = actualVector.getData();
        assertVectorDecoding(
                expectedVectorData,
                expectedDType,
                actualVectorData,
                actualVector);
        assertEquals(expectedPadding, actualVector.getPadding());
    }

    private static void assertVectorDecoding(final float[] expectedVectorData,
                                      final BinaryVector.DataType expectedDType,
                                      final Float32BinaryVector actualVector) {
        float[] actualVectorArray = actualVector.getData();
        Assertions.assertArrayEquals(actualVectorArray, expectedVectorData,
                () -> "Actual: " + Arrays.toString(actualVectorArray) + " !=  Expected:" + Arrays.toString(expectedVectorData));
        assertEquals(expectedDType, actualVector.getDataType());
    }

    private static byte[] toByteArray(final BsonArray arrayVector) {
        byte[] bytes = new byte[arrayVector.size()];
        for (int i = 0; i < arrayVector.size(); i++) {
            bytes[i] = (byte) arrayVector.get(i).asInt32().getValue();
        }
        return bytes;
    }

    private static float[] toFloatArray(final BsonArray arrayVector) {
        float[] floats = new float[arrayVector.size()];
        for (int i = 0; i < arrayVector.size(); i++) {
            BsonValue bsonValue = arrayVector.get(i);
            if (bsonValue.isString()) {
                floats[i] = Float.parseFloat(bsonValue.asString().getValue());
            } else {
                floats[i] = (float) arrayVector.get(i).asDouble().getValue();
            }
        }
        return floats;
    }

    private static Stream<Arguments> data() {
        List<Arguments> data = new ArrayList<>();
        for (BsonDocument testDocument : JsonPoweredTestHelper.getSpecTestDocuments("bson-binary-vector")) {
            for (BsonValue curValue : testDocument.getArray("tests", new BsonArray())) {
                BsonDocument testCaseDocument = curValue.asDocument();
                data.add(Arguments.of(createTestCaseDescription(testDocument, testCaseDocument), testDocument, testCaseDocument));
            }
        }
        return data.stream();
    }

    private static String createTestCaseDescription(final BsonDocument testDocument,
                                                    final BsonDocument testCaseDocument) {
        boolean isValidTestCase = testCaseDocument.getBoolean("valid").getValue();
        String fileDescription = testDocument.getString("description").getValue();
        String testDescription = testCaseDocument.getString("description").getValue();
        return "[Valid input: " + isValidTestCase + "] " + fileDescription + ": " + testDescription;
    }
}
