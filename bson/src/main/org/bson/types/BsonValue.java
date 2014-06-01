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

package org.bson.types;

import org.bson.BSONInvalidOperationException;
import org.bson.BSONType;

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
    public abstract BSONType getBsonType();

    /**
     * Gets this value as a BsonDocument if it is one, otherwise throws exception
     *
     * @return a BsonDocument
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonDocument asDocument() {
        throwIfInvalidType(BSONType.DOCUMENT);
        return (BsonDocument) this;
    }

    /**
     * Gets this value as a BsonArray if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonArray asArray() {
        throwIfInvalidType(BSONType.ARRAY);
        return (BsonArray) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonString asString() {
        throwIfInvalidType(BSONType.STRING);
        return (BsonString) this;
    }

    /**
     * Gets this value as a BsonNumber if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonNumber asNumber() {
        if (getBsonType() != BSONType.INT32 && getBsonType() != BSONType.INT64 && getBsonType() != BSONType.DOUBLE) {
            throw new BSONInvalidOperationException(String.format("Value expected to be of a numerical BSON type is of unexpected type %s",
                                                                  getBsonType()));
        }
        return (BsonNumber) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonInt32 asInt32() {
        throwIfInvalidType(BSONType.INT32);
        return (BsonInt32) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonInt64 asInt64() {
        throwIfInvalidType(BSONType.INT64);
        return (BsonInt64) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonDouble asDouble() {
        throwIfInvalidType(BSONType.DOUBLE);
        return (BsonDouble) this;
    }

    /**
     * Gets this value as a BsonString if it is one, otherwise throws exception
     *
     * @return a BsonArray
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonBoolean asBoolean() {
        throwIfInvalidType(BSONType.BOOLEAN);
        return (BsonBoolean) this;
    }

    /**
     * Gets this value as an ObjectId if it is one, otherwise throws exception
     *
     * @return an ObjectId
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public ObjectId asObjectId() {
        throwIfInvalidType(BSONType.OBJECT_ID);
        return (ObjectId) this;
    }

    /**
     * Gets this value as a DBPointer if it is one, otherwise throws exception
     *
     * @return an DBPointer
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public DBPointer asDBPointer() {
        throwIfInvalidType(BSONType.DB_POINTER);
        return (DBPointer) this;
    }

    /**
     * Gets this value as a Timestamp if it is one, otherwise throws exception
     *
     * @return an Timestamp
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public Timestamp asTimestamp() {
        throwIfInvalidType(BSONType.TIMESTAMP);
        return (Timestamp) this;
    }

    /**
     * Gets this value as a Binary if it is one, otherwise throws exception
     *
     * @return an Binary
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public Binary asBinary() {
        throwIfInvalidType(BSONType.BINARY);
        return (Binary) this;
    }

    /**
     * Gets this value as a BsonDateTime if it is one, otherwise throws exception
     *
     * @return an BsonDateTime
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public BsonDateTime asDateTime() {
        throwIfInvalidType(BSONType.DATE_TIME);
        return (BsonDateTime) this;
    }

    /**
     * Gets this value as a Symbol if it is one, otherwise throws exception
     *
     * @return an Symbol
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public Symbol asSymbol() {
        throwIfInvalidType(BSONType.SYMBOL);
        return (Symbol) this;
    }

    /**
     * Gets this value as a RegularExpression if it is one, otherwise throws exception
     *
     * @return an ObjectId
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public RegularExpression asRegularExpression() {
        throwIfInvalidType(BSONType.REGULAR_EXPRESSION);
        return (RegularExpression) this;
    }

    /**
     * Gets this value as a Code if it is one, otherwise throws exception
     *
     * @return a Code
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public Code asJavaScript() {
        throwIfInvalidType(BSONType.JAVASCRIPT);
        return (Code) this;
    }

    /**
     * Gets this value as a CodeWithScope if it is one, otherwise throws exception
     *
     * @return a CodeWithScope
     * @throws org.bson.BSONInvalidOperationException if this value is not of the expected type
     */
    public CodeWithScope asJavaScriptWithScope() {
        throwIfInvalidType(BSONType.JAVASCRIPT_WITH_SCOPE);
        return (CodeWithScope) this;
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
     * Returns true if this is a BsonDouble, false otherwise.
     *
     * @return true if this is a BsonDouble, false otherwise
     */
    public boolean isDouble() {
        return this instanceof BsonDouble;

    }

    /**
     * Returns true if this is a , false otherwise.
     *
     * @return true if this is a , false otherwise
     */
    public boolean isBoolean() {
        return this instanceof BsonBoolean;

    }

    /**
     * Returns true if this is an ObjectId, false otherwise.
     *
     * @return true if this is an ObjectId, false otherwise
     */
    public boolean isObjectId() {
        return this instanceof ObjectId;
    }

    /**
     * Returns true if this is a DBPointer, false otherwise.
     *
     * @return true if this is a DBPointer, false otherwise
     */
    public boolean isDBPointer() {
        return this instanceof DBPointer;
    }

    /**
     * Returns true if this is a Timestamp, false otherwise.
     *
     * @return true if this is a Timestamp, false otherwise
     */
    public boolean isTimestamp() {
        return this instanceof Timestamp;
    }

    /**
     * Returns true if this is a Binary, false otherwise.
     *
     * @return true if this is a Binary, false otherwise
     */
    public boolean isBinary() {
        return this instanceof Binary;
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
     * Returns true if this is a Symbol, false otherwise.
     *
     * @return true if this is a Symbol, false otherwise
     */
    public boolean isSymbol() {
        return this instanceof Symbol;
    }

    /**
     * Returns true if this is a RegularExpression, false otherwise.
     *
     * @return true if this is a RegularExpression, false otherwise
     */
    public boolean isRegularExpression() {
        return this instanceof RegularExpression;
    }

    /**
     * Returns true if this is a Code, false otherwise.
     *
     * @return true if this is a Code, false otherwise
     */
    public boolean isJavaScript() {
        return this instanceof Code;
    }

    /**
     * Returns true if this is a CodeWithScope, false otherwise.
     *
     * @return true if this is a CodeWithScope, false otherwise
     */
    public boolean isJavaScriptWithScope() {
        return this instanceof CodeWithScope;
    }

    private void throwIfInvalidType(final BSONType expectedType) {
        if (getBsonType() != expectedType) {
            throw new BSONInvalidOperationException(String.format("Value expected to be of type %s is of unexpected type %s",
                                                                  expectedType, getBsonType()));
        }
    }
}
