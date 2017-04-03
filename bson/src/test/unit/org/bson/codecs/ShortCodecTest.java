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
import org.bson.BsonType;
import org.bson.Document;
import org.junit.Test;

import java.util.HashMap;

public final class ShortCodecTest extends CodecTestCase {

    DocumentCodecProvider getDocumentCodecProvider() {
        HashMap<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
        replacements.put(BsonType.INT32, Short.class);
        return new DocumentCodecProvider(new BsonTypeClassMap(replacements));
    }

    @Test
    public void shouldRoundTripFloatValues() {
        roundTrip(new Document("a", new Short("1")));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMinRange() {
        roundTrip(new Document("a", Integer.MIN_VALUE));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldErrorDecodingOutsideMaxRange() {
        roundTrip(new Document("a", Integer.MAX_VALUE));
    }
}
