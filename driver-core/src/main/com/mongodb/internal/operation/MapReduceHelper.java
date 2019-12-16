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

package com.mongodb.internal.operation;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

final class MapReduceHelper {

    static MapReduceStatistics createStatistics(final BsonDocument result) {
        return new MapReduceStatistics(getInputCount(result), getOutputCount(result), getEmitCount(result),
                getDuration(result));
    }

    private static int getInputCount(final BsonDocument result) {
        return result.getDocument("counts", new BsonDocument()).getNumber("input", new BsonInt32(0)).intValue();
    }

    private static int getOutputCount(final BsonDocument result) {
        return result.getDocument("counts", new BsonDocument()).getNumber("output", new BsonInt32(0)).intValue();
    }

    private static int getEmitCount(final BsonDocument result) {
        return result.getDocument("counts", new BsonDocument()).getNumber("emit", new BsonInt32(0)).intValue();
    }

    private static int getDuration(final BsonDocument result) {
        return result.getNumber("timeMillis", new BsonInt32(0)).intValue();
    }

    private MapReduceHelper() {
    }
}
