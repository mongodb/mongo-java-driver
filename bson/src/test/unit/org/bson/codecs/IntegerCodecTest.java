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

package org.bson.codecs;

import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public final class IntegerCodecTest extends CodecTestCase {

    @Test
    public void shouldRoundTripIntegerValues() {
        roundTrip(new Document("a", Integer.MAX_VALUE));
        roundTrip(new Document("a", Integer.MIN_VALUE));
    }

    @Test
    public void shouldHandleAlternativeNumberValues() {
        Document expected = new Document("a", 10);
        roundTrip(new Document("a", 10L), expected);
        roundTrip(new Document("a", 10.00), expected);
        roundTrip(new Document("a", 9.9999999999999992), expected);
        roundTrip(new Document("a", Decimal128.parse("10")), expected);
    }

    @Test
    public void shouldErrorDecodingOutsideMinRange() {
        assertThrows(BsonInvalidOperationException.class, () -> roundTrip(new Document("a", Long.MIN_VALUE)));
    }

    @Test
    public void shouldErrorDecodingOutsideMaxRange() {
        assertThrows(BsonInvalidOperationException.class, () -> roundTrip(new Document("a", Long.MAX_VALUE)));
    }

    @Test
    public void shouldThrowWhenHandlingLossyDoubleValues() {
        assertThrows(BsonInvalidOperationException.class, () -> roundTrip(new Document("a", 9.9999999999999991)));
    }

    @Test
    public void shouldThrowWhenHandlingLossyDecimal128Values() {
        assertThrows(BsonInvalidOperationException.class, () -> roundTrip(new Document("a", Decimal128.parse("10.0"))));
    }

    @Override
    DocumentCodecProvider getDocumentCodecProvider() {
        return getSpecificNumberDocumentCodecProvider(Integer.class);
    }
}
