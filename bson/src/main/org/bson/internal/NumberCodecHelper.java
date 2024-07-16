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

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

import static java.lang.String.format;

/**
 * This class is not part of the public API. It may be removed or changed at any time.
 */
public final class NumberCodecHelper {

    public static byte decodeByte(final BsonReader reader) {
        int value = decodeInt(reader);
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new BsonInvalidOperationException(format("%s can not be converted into a Byte.", value));
        }
        return (byte) value;
    }

    public static short decodeShort(final BsonReader reader) {
        int value = decodeInt(reader);
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new BsonInvalidOperationException(format("%s can not be converted into a Short.", value));
        }
        return (short) value;
    }

    public static int decodeInt(final BsonReader reader) {
        int intValue;
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case INT32:
                intValue = reader.readInt32();
                break;
            case INT64:
                long longValue = reader.readInt64();
                intValue = (int) longValue;
                if (longValue != (long) intValue) {
                    throw invalidConversion(Integer.class, longValue);
                }
                break;
            case DOUBLE:
                double doubleValue = reader.readDouble();
                intValue = (int) doubleValue;
                if (doubleValue != (double) intValue) {
                    throw invalidConversion(Integer.class, doubleValue);
                }
                break;
            case DECIMAL128:
                Decimal128 decimal128 = reader.readDecimal128();
                intValue = decimal128.intValue();
                if (!decimal128.equals(new Decimal128(intValue))) {
                    throw invalidConversion(Integer.class, decimal128);
                }
                break;
            default:
                throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
        }
        return intValue;
    }

    public static long decodeLong(final BsonReader reader) {
        long longValue;
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case INT32:
                longValue = reader.readInt32();
                break;
            case INT64:
                longValue = reader.readInt64();
                break;
            case DOUBLE:
                double doubleValue = reader.readDouble();
                longValue = (long) doubleValue;
                if (doubleValue != (double) longValue) {
                    throw invalidConversion(Long.class, doubleValue);
                }
                break;
            case DECIMAL128:
                Decimal128 decimal128 = reader.readDecimal128();
                longValue = decimal128.longValue();
                if (!decimal128.equals(new Decimal128(longValue))) {
                    throw invalidConversion(Long.class, decimal128);
                }
                break;
            default:
                throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
        }
        return longValue;
    }

    public static float decodeFloat(final BsonReader reader) {
        double value = decodeDouble(reader);
        if (value < -Float.MAX_VALUE || value > Float.MAX_VALUE) {
            throw new BsonInvalidOperationException(format("%s can not be converted into a Float.", value));
        }
        return (float) value;
    }

   public static double decodeDouble(final BsonReader reader) {
        double doubleValue;
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case INT32:
                doubleValue = reader.readInt32();
                break;
            case INT64:
                long longValue = reader.readInt64();
                doubleValue = longValue;
                if (longValue != (long) doubleValue) {
                    throw invalidConversion(Double.class, longValue);
                }
                break;
            case DOUBLE:
                doubleValue = reader.readDouble();
                break;
            case DECIMAL128:
                Decimal128 decimal128 = reader.readDecimal128();
                try {
                    doubleValue = decimal128.doubleValue();
                    if (!decimal128.equals(new Decimal128(new BigDecimal(doubleValue)))) {
                        throw invalidConversion(Double.class, decimal128);
                    }
                } catch (NumberFormatException e) {
                    throw invalidConversion(Double.class, decimal128);
                }
                break;
            default:
                throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
        }
        return doubleValue;
    }

    private static  <T extends Number> BsonInvalidOperationException invalidConversion(final Class<T> clazz, final Number value) {
        return new BsonInvalidOperationException(format("Could not convert `%s` to a %s without losing precision", value, clazz));
    }

    private NumberCodecHelper() {
    }
}
