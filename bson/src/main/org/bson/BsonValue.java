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

package org.bson;

import static java.lang.String.format;

/**
 * Base class for any BSON type.
 *
 * @since 3.0
 */
public abstract class BsonValue {
    /**
     * Construct a new instance.  This is package-protected so that the BSON type system is closed.
     */
    BsonValue() {
    }

    /**
     * Gets the BSON type of this value.
     *
     * @return the BSON type, which may not be null (but may be BSONType.NULL)
     */
    public abstract BsonType getBsonType();

    /**
     * Gets this value as a BsonDocument if it is one, otherwise throws exception
     *
     * @return a BsonDocument
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonDocument asDocument() {
        throwIfInvalidType(BsonType.DOCUMENT);
        return (BsonDocument) this;
    }

    /**
     * Gets this value as a BsonArray if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonArray asArray() {
        throwIfInvalidType(BsonType.ARRAY);
        return (BsonArray) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonString
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonString asString() {
        throwIfInvalidType(BsonType.STRING);
        return (BsonString) this;
    }

    /**
     * Gets this value as a BsonNumber if it is one, otherwise throws exception
     *
     * @return a BsonNumber
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonNumber asNumber() {
        if (getBsonType() != BsonType.INT32 && getBsonType() != BsonType.INT64 && getBsonType() != BsonType.DOUBLE) {
            throw new BsonInvalidOperationException(format("Value expected to be of a numerical BSON type is of unexpected type %s",
                                                           getBsonType()));
        }
        return (BsonNumber) this;
    }

    /**
     * Gets this value as a BsonInt32 if it is one, otherwise throws exception
     *
     * @return a BsonInt32
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonInt32 asInt32() {
        throwIfInvalidType(BsonType.INT32);
        return (BsonInt32) this;
    }

    /**
     * Gets this value as a BsonInt64 if it is one, otherwise throws exception
     *
     * @return a BsonInt64
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonInt64 asInt64() {
        throwIfInvalidType(BsonType.INT64);
        return (BsonInt64) this;
    }

    /**
     * Gets this value as a BsonDecimal128 if it is one, otherwise throws exception
     *
     * @return a BsonDecimal128
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     * @since 3.4
     */
    public BsonDecimal128 asDecimal128() {
        throwIfInvalidType(BsonType.DECIMAL128);
        return (BsonDecimal128) this;
    }

    /**
     * Gets this value as a BsonDouble if it is one, otherwise throws exception
     *
     * @return a BsonDouble
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonDouble asDouble() {
        throwIfInvalidType(BsonType.DOUBLE);
        return (BsonDouble) this;
    }

    /**
     * Gets this value as a BsonBoolean if it is one, otherwise throws exception
     *
     * @return a BsonBoolean
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonBoolean asBoolean() {
        throwIfInvalidType(BsonType.BOOLEAN);
        return (BsonBoolean) this;
    }

    /**
     * Gets this value as an BsonObjectId if it is one, otherwise throws exception
     *
     * @return an BsonObjectId
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonObjectId asObjectId() {
        throwIfInvalidType(BsonType.OBJECT_ID);
        return (BsonObjectId) this;
    }

    /**
     * Gets this value as a BsonDbPointer if it is one, otherwise throws exception
     *
     * @return an BsonDbPointer
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonDbPointer asDBPointer() {
        throwIfInvalidType(BsonType.DB_POINTER);
        return (BsonDbPointer) this;
    }

    /**
     * Gets this value as a BsonTimestamp if it is one, otherwise throws exception
     *
     * @return an BsonTimestamp
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonTimestamp asTimestamp() {
        throwIfInvalidType(BsonType.TIMESTAMP);
        return (BsonTimestamp) this;
    }

    /**
     * Gets this value as a BsonBinary if it is one, otherwise throws exception
     *
     * @return an BsonBinary
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonBinary asBinary() {
        throwIfInvalidType(BsonType.BINARY);
        return (BsonBinary) this;
    }

    /**
     * Gets this value as a BsonDateTime if it is one, otherwise throws exception
     *
     * @return an BsonDateTime
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonDateTime asDateTime() {
        throwIfInvalidType(BsonType.DATE_TIME);
        return (BsonDateTime) this;
    }

    /**
     * Gets this value as a BsonSymbol if it is one, otherwise throws exception
     *
     * @return an BsonSymbol
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonSymbol asSymbol() {
        throwIfInvalidType(BsonType.SYMBOL);
        return (BsonSymbol) this;
    }

    /**
     * Gets this value as a BsonRegularExpression if it is one, otherwise throws exception
     *
     * @return an BsonRegularExpression
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonRegularExpression asRegularExpression() {
        throwIfInvalidType(BsonType.REGULAR_EXPRESSION);
        return (BsonRegularExpression) this;
    }

    /**
     * Gets this value as a {@code BsonJavaScript} if it is one, otherwise throws exception
     *
     * @return a BsonJavaScript
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonJavaScript asJavaScript() {
        throwIfInvalidType(BsonType.JAVASCRIPT);
        return (BsonJavaScript) this;
    }

    /**
     * Gets this value as a BsonJavaScriptWithScope if it is one, otherwise throws exception
     *
     * @return a BsonJavaScriptWithScope
     * @throws org.bson.BsonInvalidOperationException if this value is not of the expected type
     */
    public BsonJavaScriptWithScope asJavaScriptWithScope() {
        throwIfInvalidType(BsonType.JAVASCRIPT_WITH_SCOPE);
        return (BsonJavaScriptWithScope) this;
    }


    /**
     * Returns true if this is a BsonNull, false otherwise.
     *
     * @return true if this is a BsonNull, false otherwise
     */
    public boolean isNull() {
        return this instanceof BsonNull;
    }

    /**
     * Returns true if this is a BsonDocument, false otherwise.
     *
     * @return true if this is a BsonDocument, false otherwise
     */
    public boolean isDocument() {
        return this instanceof BsonDocument;
    }

    /**
     * Returns true if this is a BsonArray, false otherwise.
     *
     * @return true if this is a BsonArray, false otherwise
     */
    public boolean isArray() {
        return this instanceof BsonArray;
    }

    /**
     * Returns true if this is a BsonString, false otherwise.
     *
     * @return true if this is a BsonString, false otherwise
     */
    public boolean isString() {
        return this instanceof BsonString;
    }

    /**
     * Returns true if this is a BsonNumber, false otherwise.
     *
     * @return true if this is a BsonNumber, false otherwise
     */
    public boolean isNumber() {
        return isInt32() || isInt64() || isDouble();
    }

    /**
     * Returns true if this is a BsonInt32, false otherwise.
     *
     * @return true if this is a BsonInt32, false otherwise
     */
    public boolean isInt32() {
        return this instanceof BsonInt32;
    }

    /**
     * Returns true if this is a BsonInt64, false otherwise.
     *
     * @return true if this is a BsonInt64, false otherwise
     */
    public boolean isInt64() {
        return this instanceof BsonInt64;
    }

    /**
     * Returns true if this is a BsonDecimal128, false otherwise.
     *
     * @return true if this is a BsonDecimal128, false otherwise
     * @since 3.4
     */
    public boolean isDecimal128() {
        return this instanceof BsonDecimal128;
    }

    /**
     * Returns true if this is a BsonDouble, false otherwise.
     *
     * @return true if this is a BsonDouble, false otherwise
     */
    public boolean isDouble() {
        return this instanceof BsonDouble;

    }

    /**
     * Returns true if this is a BsonBoolean, false otherwise.
     *
     * @return true if this is a BsonBoolean, false otherwise
     */
    public boolean isBoolean() {
        return this instanceof BsonBoolean;

    }

    /**
     * Returns true if this is an BsonObjectId, false otherwise.
     *
     * @return true if this is an BsonObjectId, false otherwise
     */
    public boolean isObjectId() {
        return this instanceof BsonObjectId;
    }

    /**
     * Returns true if this is a BsonDbPointer, false otherwise.
     *
     * @return true if this is a BsonDbPointer, false otherwise
     */
    public boolean isDBPointer() {
        return this instanceof BsonDbPointer;
    }

    /**
     * Returns true if this is a BsonTimestamp, false otherwise.
     *
     * @return true if this is a BsonTimestamp, false otherwise
     */
    public boolean isTimestamp() {
        return this instanceof BsonTimestamp;
    }

    /**
     * Returns true if this is a BsonBinary, false otherwise.
     *
     * @return true if this is a BsonBinary, false otherwise
     */
    public boolean isBinary() {
        return this instanceof BsonBinary;
    }

    /**
     * Returns true if this is a BsonDateTime, false otherwise.
     *
     * @return true if this is a BsonDateTime, false otherwise
     */
    public boolean isDateTime() {
        return this instanceof BsonDateTime;
    }

    /**
     * Returns true if this is a BsonSymbol, false otherwise.
     *
     * @return true if this is a BsonSymbol, false otherwise
     */
    public boolean isSymbol() {
        return this instanceof BsonSymbol;
    }

    /**
     * Returns true if this is a BsonRegularExpression, false otherwise.
     *
     * @return true if this is a BsonRegularExpression, false otherwise
     */
    public boolean isRegularExpression() {
        return this instanceof BsonRegularExpression;
    }

    /**
     * Returns true if this is a BsonJavaScript, false otherwise.
     *
     * @return true if this is a BsonJavaScript, false otherwise
     */
    public boolean isJavaScript() {
        return this instanceof BsonJavaScript;
    }

    /**
     * Returns true if this is a BsonJavaScriptWithScope, false otherwise.
     *
     * @return true if this is a BsonJavaScriptWithScope, false otherwise
     */
    public boolean isJavaScriptWithScope() {
        return this instanceof BsonJavaScriptWithScope;
    }

    private void throwIfInvalidType(final BsonType expectedType) {
        if (getBsonType() != expectedType) {
            throw new BsonInvalidOperationException(format("Value expected to be of type %s is of unexpected type %s",
                                                           expectedType, getBsonType()));
        }
    }
}
