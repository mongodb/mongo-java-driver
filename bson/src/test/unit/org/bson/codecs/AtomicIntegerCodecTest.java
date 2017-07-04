/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public final class AtomicIntegerCodecTest extends CodecTestCase {

    @Test
    public void shouldRoundTripAtomicIntegerValues() {
        Document original = new Document("a", new AtomicInteger(Integer.MAX_VALUE));
        roundTrip(original, new AtomicIntegerComparator(original));

        original = new Document("a", new AtomicInteger(Integer.MIN_VALUE));
        roundTrip(original, new AtomicIntegerComparator(original));
    }

    @Test
    public void shouldHandleAlternativeNumberValues() {
        Document expected = new Document("a", new AtomicInteger(10));
        roundTrip(new Document("a", 10), new AtomicIntegerComparator(expected));
        roundTrip(new Document("a", 10L), new AtomicIntegerComparator(expected));
        roundTrip(new Document("a", 10.00), new AtomicIntegerComparator(expected));
        roundTrip(new Document("a", 9.9999999999999992), new AtomicIntegerComparator(expected));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowWhenHandlingLossyDoubleValues() {
        Document original = new Document("a", 9.9999999999999991);
        roundTrip(original, new AtomicIntegerComparator(original));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMinRange() {
        roundTrip(new Document("a", Long.MIN_VALUE));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMaxRange() {
        roundTrip(new Document("a", Long.MAX_VALUE));
    }

    @Override
    DocumentCodecProvider getDocumentCodecProvider() {
        return getSpecificNumberDocumentCodecProvider(AtomicInteger.class);
    }

    private class AtomicIntegerComparator implements Comparator<Document> {
        private final Document expected;

        AtomicIntegerComparator(final Document expected) {
            this.expected = expected;
        }

        @Override
        public void apply(final Document result) {
            assertEquals("Codec Round Trip",
                    expected.get("a", AtomicInteger.class).get(),
                    result.get("a", AtomicInteger.class).get());
        }
    }

}
