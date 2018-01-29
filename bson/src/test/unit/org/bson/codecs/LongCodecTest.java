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
import org.junit.Test;

public final class LongCodecTest extends CodecTestCase {

    @Test
    public void shouldRoundTripLongValues() {
        roundTrip(new Document("a", Long.MAX_VALUE));
        roundTrip(new Document("a", Long.MIN_VALUE));
    }

    @Test
    public void shouldHandleAlternativeNumberValues() {
        Document expected = new Document("a", 10L);
        roundTrip(new Document("a", 10), expected);
        roundTrip(new Document("a", 10.00), expected);
        roundTrip(new Document("a", 9.9999999999999992), expected);
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowWhenHandlingLossyValues() {
        roundTrip(new Document("a", Double.MAX_VALUE));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowWhenHandlingLossyDoubleValues() {
        roundTrip(new Document("a", 9.9999999999999991));
    }

    @Override
    DocumentCodecProvider getDocumentCodecProvider() {
        return getSpecificNumberDocumentCodecProvider(Long.class);
    }
}
