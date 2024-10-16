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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Float32Vector;
import org.bson.PackedBitVector;
import org.bson.Vector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.bson.BsonHelper.decodeToDocument;
import static org.bson.BsonHelper.encodeToHex;
import static org.bson.internal.vector.VectorHelper.determineVectorDType;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

// BSON tests powered by language-agnostic JSON-based tests included in test resources
class VectorGenericBsonTest {

    private static final List<String> TEST_NAMES_TO_IGNORE = Arrays.asList(
            //NO API to set padding for Floats available
            "FLOAT32 with padding",
            //NO API to set padding for Floats available
            "INT8 with padding",
            //It is impossible to provide float inputs for INT8 in the API
            "INT8 with float inputs",
            //It is impossible to provide float inputs for INT8
            "Underflow Vector PACKED_BIT",
            //It is impossible to provide float inputs for PACKED_BIT in the API
            "Vector with float values PACKED_BIT",
            //It is impossible to provide float inputs for INT8
            "Overflow Vector PACKED_BIT",
            "Overflow Vector INT8",
            // It is impossible to provide -129 for byte.
            "Underflow Vector INT8");


    @ParameterizedTest(name = "{0}")
    @MethodSource("provideTestCases")
    void shouldPassAllOutcomes(@SuppressWarnings("unused") final String description,
                               final BsonDocument testDefinition, final BsonDocument testCase) {
        assumeFalse(TEST_NAMES_TO_IGNORE.contains(testCase.get("description").asString().getValue()));

        String testKey = testDefinition.getString("test_key").getValue();
        boolean isValidVector = testCase.getBoolean("valid").getValue();
        if (isValidVector) {
            runValidTestCase(testKey, testCase);
        } else {
            runInvalidTestCase(testCase);
        }
    }

    private void runInvalidTestCase(final BsonDocument testCase) {
        BsonArray arrayVector = testCase.getArray("vector");
        byte expectedPadding = (byte) testCase.getInt32("padding").getValue();
        byte dtypeByte = Byte.decode(testCase.getString("dtype_hex").getValue());
        Vector.Dtype expectedDType = determineVectorDType(dtypeByte);

        switch (expectedDType) {
            case INT8:
                byte[] expectedVectorData = toByteArray(arrayVector);
                assertValidationException(assertThrows(RuntimeException.class,
                        () -> Vector.int8Vector(expectedVectorData)));
                break;
            case PACKED_BIT:
                byte[] expectedVectorPackedBitData = toByteArray(arrayVector);
                assertValidationException(assertThrows(RuntimeException.class,
                        () -> Vector.packedBitVector(expectedVectorPackedBitData, expectedPadding)));
                break;
            case FLOAT32:
                float[] expectedFloatVector = toFloatArray(arrayVector);
                assertValidationException(assertThrows(RuntimeException.class, () -> Vector.floatVector(expectedFloatVector)));
                break;
            default:
                throw new IllegalArgumentException("Unsupported vector data type: " + expectedDType);
        }
    }

    private void runValidTestCase(final String testKey, final BsonDocument testCase) {
        String description = testCase.getString("description").getValue();
        byte dtypeByte = Byte.decode(testCase.getString("dtype_hex").getValue());

        byte expectedPadding = (byte) testCase.getInt32("padding").getValue();
        Vector.Dtype expectedDType = determineVectorDType(dtypeByte);
        String expectedCanonicalBsonHex = testCase.getString("canonical_bson").getValue().toUpperCase();

        BsonArray arrayVector = testCase.getArray("vector");
        BsonDocument actualDecodedDocument = decodeToDocument(expectedCanonicalBsonHex, description);
        Vector actualVector = actualDecodedDocument.getBinary("vector").asVector();

        switch (expectedDType) {
            case INT8:
                byte[] expectedVectorData = toByteArray(arrayVector);
                byte[] actualVectorData = actualVector.asInt8Vector().getVectorArray();
                assertVectorDecoding(
                        expectedCanonicalBsonHex,
                        expectedVectorData,
                        expectedDType,
                        actualDecodedDocument,
                        actualVectorData,
                        actualVector);

                assertThatVectorCreationResultsInCorrectBinary(Vector.int8Vector(expectedVectorData),
                        testKey,
                        actualDecodedDocument,
                        expectedCanonicalBsonHex,
                        description);
                break;
            case PACKED_BIT:
                PackedBitVector actualPackedBitVector = actualVector.asPackedBitVector();
                byte[] expectedVectorPackedBitData = toByteArray(arrayVector);
                assertVectorDecoding(
                        expectedCanonicalBsonHex, expectedVectorPackedBitData,
                        expectedDType, expectedPadding,
                        actualDecodedDocument,
                        actualPackedBitVector);

                assertThatVectorCreationResultsInCorrectBinary(
                        Vector.packedBitVector(expectedVectorPackedBitData, expectedPadding),
                        testKey,
                        actualDecodedDocument,
                        expectedCanonicalBsonHex,
                        description);
                break;
            case FLOAT32:
                Float32Vector actualFloat32Vector = actualVector.asFloat32Vector();
                float[] expectedFloatVector = toFloatArray(arrayVector);
                assertVectorDecoding(
                        expectedCanonicalBsonHex,
                        expectedFloatVector,
                        expectedDType,
                        actualDecodedDocument,
                        actualFloat32Vector);
                assertThatVectorCreationResultsInCorrectBinary(
                        Vector.floatVector(expectedFloatVector),
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

    private static void assertThatVectorCreationResultsInCorrectBinary(final Vector expectedVectorData,
                                                                       final String testKey,
                                                                       final BsonDocument actualDecodedDocument,
                                                                       final String expectedCanonicalBsonHex,
                                                                       final String description) {
        BsonDocument documentToEncode = new BsonDocument(testKey, new BsonBinary(expectedVectorData));
        assertEquals(documentToEncode, actualDecodedDocument);
        assertEquals(expectedCanonicalBsonHex, encodeToHex(documentToEncode),
                format("Failed to create expected BSON for document with description '%s'", description));
    }

    private void assertVectorDecoding(final String expectedCanonicalBsonHex,
                                      final byte[] expectedVectorData,
                                      final Vector.Dtype expectedDType,
                                      final BsonDocument actualDecodedDocument,
                                      final byte[] actualVectorData,
                                      final Vector actualVector) {
        assertEquals(expectedCanonicalBsonHex, encodeToHex(actualDecodedDocument));
        Assertions.assertArrayEquals(actualVectorData, expectedVectorData,
                () -> "Actual: " + Arrays.toString(actualVectorData) + " !=  Expected:" + Arrays.toString(expectedVectorData));
        assertEquals(expectedDType, actualVector.getDataType());
    }

    private void assertVectorDecoding(final String expectedCanonicalBsonHex,
                                      final byte[] expectedVectorData,
                                      final Vector.Dtype expectedDType,
                                      final byte expectedPadding,
                                      final BsonDocument actualDecodedDocument,
                                      final PackedBitVector actualVector) {
        byte[] actualVectorData = actualVector.getVectorArray();
        assertVectorDecoding(expectedCanonicalBsonHex,
                expectedVectorData,
                expectedDType,
                actualDecodedDocument,
                actualVectorData,
                actualVector);
        assertEquals(expectedPadding, actualVector.getPadding());
    }

    private void assertVectorDecoding(final String expectedCanonicalBsonHex,
                                      final float[] expectedVectorData,
                                      final Vector.Dtype expectedDType,
                                      final BsonDocument actualDecodedDocument,
                                      final Float32Vector actualVector) {
        assertEquals(expectedCanonicalBsonHex, encodeToHex(actualDecodedDocument));
        float[] actualVectorArray = actualVector.getVectorArray();
        Assertions.assertArrayEquals(actualVectorArray, expectedVectorData,
                () -> "Actual: " + Arrays.toString(actualVectorArray) + " !=  Expected:" + Arrays.toString(expectedVectorData));
        assertEquals(expectedDType, actualVector.getDataType());
    }

    private byte[] toByteArray(final BsonArray arrayVector) {
        byte[] bytes = new byte[arrayVector.size()];
        for (int i = 0; i < arrayVector.size(); i++) {
            bytes[i] = (byte) arrayVector.get(i).asInt32().getValue();
        }
        return bytes;
    }

    private float[] toFloatArray(final BsonArray arrayVector) {
        float[] floats = new float[arrayVector.size()];
        for (int i = 0; i < arrayVector.size(); i++) {
            BsonValue bsonValue = arrayVector.get(i);
            if (bsonValue.isString()) {
                floats[i] = parseFloat(bsonValue.asString());
            } else {
                floats[i] = (float) arrayVector.get(i).asDouble().getValue();
            }
        }
        return floats;
    }

    private static float parseFloat(final BsonString bsonValue) {
        String floatValue = bsonValue.getValue();
        switch (floatValue) {
            case "-inf":
                return Float.NEGATIVE_INFINITY;
            case "inf":
                return Float.POSITIVE_INFINITY;
            default:
                return Float.parseFloat(floatValue);
        }
    }

    private static Stream<Arguments> provideTestCases() throws URISyntaxException, IOException {
        List<Arguments> data = new ArrayList<>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/bson-binary-vector")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
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
        String testSuiteDescription = testDocument.getString("description").getValue();
        String testCaseDescription = testCaseDocument.getString("description").getValue();
        return "[Valid input: " + isValidTestCase + "] " + testSuiteDescription + ": " + testCaseDescription;
    }
}
