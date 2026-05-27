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
package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HnswSearchIndexOptionsTest {

    @Test
    void emptyOptions() {
        assertEquals(
                new BsonDocument(),
                new HnswSearchIndexOptions().toBsonDocument()
        );
    }

    @Test
    void maxEdgesOnly() {
        assertEquals(
                new BsonDocument("maxEdges", new BsonInt32(24)),
                new HnswSearchIndexOptions().maxEdges(24).toBsonDocument()
        );
    }

    @Test
    void numEdgeCandidatesOnly() {
        assertEquals(
                new BsonDocument("numEdgeCandidates", new BsonInt32(150)),
                new HnswSearchIndexOptions().numEdgeCandidates(150).toBsonDocument()
        );
    }

    @Test
    void allOptions() {
        assertEquals(
                new BsonDocument("maxEdges", new BsonInt32(16))
                        .append("numEdgeCandidates", new BsonInt32(200)),
                new HnswSearchIndexOptions().maxEdges(16).numEdgeCandidates(200).toBsonDocument()
        );
    }

    @Test
    void maxEdgesRejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> new HnswSearchIndexOptions().maxEdges(0));
    }

    @Test
    void maxEdgesRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> new HnswSearchIndexOptions().maxEdges(-1));
    }

    @Test
    void numEdgeCandidatesRejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> new HnswSearchIndexOptions().numEdgeCandidates(0));
    }

    @Test
    void numEdgeCandidatesRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> new HnswSearchIndexOptions().numEdgeCandidates(-1));
    }
}
