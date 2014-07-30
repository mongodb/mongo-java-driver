/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.codecs;

import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.mongodb.CodeWithScope;
import org.mongodb.Document;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A map from a BSON types to the Class to which it should be decoded.  This class is useful if, for example,
 * you want to change the default decoding of BSON DATE to something besides {@code java.util.Date}.
 * <p>
 * The default mappings are:
 *
 * <table>
 *     <tr>
 *         <th>BSON Type</th>
 *         <th>Class</th>
 *     </tr>
 *     <tr>
 *         <td>DOCUMENT</td>
 *         <td>{@code org.mongodb.Document.class}</td>
 *     </tr>
 *     <tr>
 *         <td>ARRAY</td>
 *         <td>{@code java.util.List.class}</td>
 *     </tr>
 *     <tr>
 *         <td>DATE_TIME</td>
 *         <td>{@code java.util.Date.class}</td>
 *     </tr>
 *     <tr>
 *         <td>BOOLEAN</td>
 *         <td>{@code java.lang.Boolean.class}</td>
 *     </tr>
 *     <tr>
 *         <td>DOUBLE</td>
 *         <td>{@code java.lang.Double.class}</td>
 *     </tr>
 *     <tr>
 *         <td>INT32</td>
 *         <td>{@code java.lang.Integer.class}</td>
 *     </tr>
 *     <tr>
 *         <td>INT64</td>
 *         <td>{@code java.lang.Long.class}</td>
 *     </tr>
 *     <tr>
 *         <td>STRING</td>
 *         <td>{@code java.lang.String.class}</td>
 *     </tr>
 *     <tr>
 *         <td>BINARY</td>
 *         <td>{@code org.bson.types.Binary.class}</td>
 *     </tr>
 *     <tr>
 *         <td>OBJECT_ID</td>
 *         <td>{@code org.bson.types.ObjectId.class}</td>
 *     </tr>
 *     <tr>
 *         <td>REGULAR_EXPRESSION</td>
 *         <td>{@code org.bson.types.RegularExpression.class}</td>
 *     </tr>
 *     <tr>
 *         <td>SYMBOL</td>
 *         <td>{@code org.bson.types.Symbol.class}</td>
 *     </tr>
 *     <tr>
 *         <td>DB_POINTER</td>
 *         <td>{@code org.bson.types.DBPointer.class}</td>
 *     </tr>
 *     <tr>
 *         <td>MAX_KEY</td>
 *         <td>{@code org.bson.types.MaxKey.class}</td>
 *     </tr>
 *     <tr>
 *         <td>MIN_KEY</td>
 *         <td>{@code org.bson.types.MinKey.class}</td>
 *     </tr>
 *     <tr>
 *         <td>JAVASCRIPT</td>
 *         <td>{@code org.bson.types.Code.class}</td>
 *     </tr>
 *     <tr>
 *         <td>JAVASCRIPT_WITH_SCOPE</td>
 *         <td>{@code org.bson.types.CodeWithScope.class}</td>
 *     </tr>
 *     <tr>
 *         <td>TIMESTAMP</td>
 *         <td>{@code org.bson.types.BSONTimestamp.class}</td>
 *     </tr>
 *     <tr>
 *         <td>UNDEFINED</td>
 *         <td>{@code org.bson.types.Undefined.class}</td>
 *     </tr>
 * </table>
 *
 * @since 3.0
 */
public class BsonTypeClassMap {
    private final Map<BsonType, Class<?>> map = new HashMap<BsonType, Class<?>>();

    /**
     * Construct an instance with the default mapping, but replacing the default mapping with any values contained in the given map.
     * This allows a caller to easily replace a single or a few mappings, while leaving the rest at their default values.
     *
     * @param replacementsForDefaults the replacement mappings
     */
    public BsonTypeClassMap(final Map<BsonType, Class<?>> replacementsForDefaults) {
        addDefaults();
        map.putAll(replacementsForDefaults);
    }

    /**
     * Construct an instance with the default mappings.
     */
    public BsonTypeClassMap() {
        this(Collections.<BsonType, Class<?>>emptyMap());
    }


    Class<?> get(final BsonType bsonType) {
        return map.get(bsonType);
    }

    private void addDefaults() {
        map.put(BsonType.ARRAY, List.class);
        map.put(BsonType.BINARY, Binary.class);
        map.put(BsonType.BOOLEAN, Boolean.class);
        map.put(BsonType.DATE_TIME, Date.class);
        map.put(BsonType.DB_POINTER, BsonDbPointer.class);
        map.put(BsonType.DOCUMENT, Document.class);
        map.put(BsonType.DOUBLE, Double.class);
        map.put(BsonType.INT32, Integer.class);
        map.put(BsonType.INT64, Long.class);
        map.put(BsonType.MAX_KEY, MaxKey.class);
        map.put(BsonType.MIN_KEY, MinKey.class);
        map.put(BsonType.JAVASCRIPT, Code.class);
        map.put(BsonType.JAVASCRIPT_WITH_SCOPE, CodeWithScope.class);
        map.put(BsonType.OBJECT_ID, ObjectId.class);
        map.put(BsonType.REGULAR_EXPRESSION, BsonRegularExpression.class);
        map.put(BsonType.STRING, String.class);
        map.put(BsonType.SYMBOL, Symbol.class);
        map.put(BsonType.TIMESTAMP, BsonTimestamp.class);
        map.put(BsonType.UNDEFINED, BsonUndefined.class);
    }
}
