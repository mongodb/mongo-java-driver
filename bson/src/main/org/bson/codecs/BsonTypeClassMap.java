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

import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>A map from a BSON types to the Class to which it should be decoded.  This class is useful if, for example,
 * you want to change the default decoding of BSON DATE to something besides {@code java.util.Date}.</p>
 *
 * <p>The default mappings are:</p>
 *
 * <ul>
 *     <li><em>DOCUMENT</em>: {@code org.bson.Document.class} </li>
 *     <li><em>ARRAY</em>: {@code java.util.List.class} </li>
 *     <li><em>DATE_TIME</em>: {@code java.util.Date.class} </li>
 *     <li><em>BOOLEAN</em>: {@code java.lang.Boolean.class} </li>
 *     <li><em>DOUBLE</em>: {@code java.lang.Double.class} </li>
 *     <li><em>INT32</em>: {@code java.lang.Integer.class} </li>
 *     <li><em>INT64</em>: {@code java.lang.Long.class} </li>
 *     <li><em>DECIMAL128</em>: {@code org.bson.types.Decimal128.class} </li>
 *     <li><em>STRING</em>: {@code java.lang.String.class} </li>
 *     <li><em>BINARY</em>: {@code org.bson.types.Binary.class} </li>
 *     <li><em>OBJECT_ID</em>: {@code org.bson.types.ObjectId.class} </li>
 *     <li><em>REGULAR_EXPRESSION</em>: {@code org.bson.types.RegularExpression.class} </li>
 *     <li><em>SYMBOL</em>: {@code org.bson.types.Symbol.class} </li>
 *     <li><em>DB_POINTER</em>: {@code org.bson.types.DBPointer.class} </li>
 *     <li><em>MAX_KEY</em>: {@code org.bson.types.MaxKey.class} </li>
 *     <li><em>MIN_KEY</em>: {@code org.bson.types.MinKey.class} </li>
 *     <li><em>JAVASCRIPT</em>: {@code org.bson.types.Code.class} </li>
 *     <li><em>JAVASCRIPT_WITH_SCOPE</em>: {@code org.bson.types.CodeWithScope.class} </li>
 *     <li><em>TIMESTAMP</em>: {@code org.bson.types.BSONTimestamp.class} </li>
 *     <li><em>UNDEFINED</em>: {@code org.bson.types.Undefined.class} </li>
 * </ul>
 *
 * @since 3.0
 */
public class BsonTypeClassMap {
    static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();
    private final Class<?>[] bsonTypeOrdinalToClassMap = new Class<?>[BsonType.values().length];

    /**
     * Construct an instance with the default mapping, but replacing the default mapping with any values contained in the given map.
     * This allows a caller to easily replace a single or a few mappings, while leaving the rest at their default values.
     *
     * @param replacementsForDefaults the replacement mappings
     */
    public BsonTypeClassMap(final Map<BsonType, Class<?>> replacementsForDefaults) {
        addDefaults();
        replacementsForDefaults.forEach((key, value) -> bsonTypeOrdinalToClassMap[key.ordinal()] = value);
    }

    /**
     * Construct an instance with the default mappings.
     */
    public BsonTypeClassMap() {
        this(Collections.emptyMap());
    }

    /**
     * Gets the Class that is mapped to the given BSON type.
     *
     * @param bsonType the BSON type
     * @return the Class that is mapped to the BSON type
     */
    public Class<?> get(final BsonType bsonType) {
        return bsonTypeOrdinalToClassMap[bsonType.ordinal()];
    }

    private void addDefaults() {
        bsonTypeOrdinalToClassMap[BsonType.ARRAY.ordinal()] = List.class;
        bsonTypeOrdinalToClassMap[BsonType.BINARY.ordinal()] = Binary.class;
        bsonTypeOrdinalToClassMap[BsonType.BOOLEAN.ordinal()] = Boolean.class;
        bsonTypeOrdinalToClassMap[BsonType.DATE_TIME.ordinal()] = Date.class;
        bsonTypeOrdinalToClassMap[BsonType.DB_POINTER.ordinal()] = BsonDbPointer.class;
        bsonTypeOrdinalToClassMap[BsonType.DOCUMENT.ordinal()] = Document.class;
        bsonTypeOrdinalToClassMap[BsonType.DOUBLE.ordinal()] = Double.class;
        bsonTypeOrdinalToClassMap[BsonType.INT32.ordinal()] = Integer.class;
        bsonTypeOrdinalToClassMap[BsonType.INT64.ordinal()] = Long.class;
        bsonTypeOrdinalToClassMap[BsonType.DECIMAL128.ordinal()] = Decimal128.class;
        bsonTypeOrdinalToClassMap[BsonType.MAX_KEY.ordinal()] = MaxKey.class;
        bsonTypeOrdinalToClassMap[BsonType.MIN_KEY.ordinal()] = MinKey.class;
        bsonTypeOrdinalToClassMap[BsonType.JAVASCRIPT.ordinal()] = Code.class;
        bsonTypeOrdinalToClassMap[BsonType.JAVASCRIPT_WITH_SCOPE.ordinal()] = CodeWithScope.class;
        bsonTypeOrdinalToClassMap[BsonType.OBJECT_ID.ordinal()] = ObjectId.class;
        bsonTypeOrdinalToClassMap[BsonType.REGULAR_EXPRESSION.ordinal()] = BsonRegularExpression.class;
        bsonTypeOrdinalToClassMap[BsonType.STRING.ordinal()] = String.class;
        bsonTypeOrdinalToClassMap[BsonType.SYMBOL.ordinal()] = Symbol.class;
        bsonTypeOrdinalToClassMap[BsonType.TIMESTAMP.ordinal()] = BsonTimestamp.class;
        bsonTypeOrdinalToClassMap[BsonType.UNDEFINED.ordinal()] = BsonUndefined.class;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonTypeClassMap that = (BsonTypeClassMap) o;

        return Arrays.equals(bsonTypeOrdinalToClassMap, that.bsonTypeOrdinalToClassMap);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bsonTypeOrdinalToClassMap);
    }
}
