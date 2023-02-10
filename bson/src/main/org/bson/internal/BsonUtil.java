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
package org.bson.internal;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonValue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class BsonUtil {
    public static BsonDocument mutableDeepCopy(final BsonDocument original) {
        BsonDocument copy = new BsonDocument(original.size());
        original.forEach((key, value) -> copy.put(key, mutableDeepCopy(value)));
        return copy;
    }

    private static BsonArray mutableDeepCopy(final BsonArray original) {
        BsonArray copy = new BsonArray(original.size());
        original.forEach(element -> copy.add(mutableDeepCopy(element)));
        return copy;
    }

    private static BsonBinary mutableDeepCopy(final BsonBinary original) {
        return new BsonBinary(original.getType(), original.getData().clone());
    }

    private static BsonJavaScriptWithScope mutableDeepCopy(final BsonJavaScriptWithScope original) {
        return new BsonJavaScriptWithScope(original.getCode(), mutableDeepCopy(original.getScope()));
    }

    private static <T extends BsonValue> T mutableDeepCopy(final T original) {
        BsonValue copy;
        switch (original.getBsonType()) {
            case DOCUMENT:
                copy = mutableDeepCopy(original.asDocument());
                break;
            case ARRAY:
                copy = mutableDeepCopy(original.asArray());
                break;
            case BINARY:
                copy = mutableDeepCopy(original.asBinary());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                copy = mutableDeepCopy(original.asJavaScriptWithScope());
                break;
            default:
                copy = original;
        }
        @SuppressWarnings("unchecked")
        T result = (T) copy;
        return result;
    }

    private BsonUtil() {
    }
}
