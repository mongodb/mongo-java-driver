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

public final class FloatCodecTest extends CodecTestCase {

    @Test
    public void shouldRoundTripFloatValues() {
        roundTrip(new Document("a", Float.MAX_VALUE));
        roundTrip(new Document("a", -Float.MAX_VALUE));
    }

    @Test
    public void shouldRoundTripNegativeFloatValues() {
        roundTrip(new Document("a", -1f));
    }

    @Test
    public void shouldHandleAlternativeNumberValues() {
        Document expected = new Document("a", 10f);
        roundTrip(new Document("a", 10), expected);
        roundTrip(new Document("a", 10L), expected);
        roundTrip(new Document("a", 9.9999999999999992), expected);
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMinRange() {
        roundTrip(new Document("a", -Double.MAX_VALUE));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMaxRange() {
        roundTrip(new Document("a", Double.MAX_VALUE));
    }

    @Override
    DocumentCodecProvider getDocumentCodecProvider() {
        return getSpecificNumberDocumentCodecProvider(Float.class);
    }
}
