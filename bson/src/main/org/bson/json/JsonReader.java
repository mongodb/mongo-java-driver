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

package org.bson.json;


import org.bson.AbstractBsonReader;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonContextType;
import org.bson.BsonDbPointer;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonReaderMark;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.io.Reader;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static java.lang.String.format;


/**
 * Reads a JSON in one of the following modes:
 * <ul>
 *  <li><em>Strict mode</em> that conforms to the <a href="http://www.json.org/">JSON RFC specifications.</a></li>
 *  <li><em>JavaScript mode</em> that that most JavaScript interpreters can process</li>
 *  <li><em>Shell mode</em> that the <a href="https://www.mongodb.com/docs/manual/reference/mongo/#mongo">mongo</a> shell can process.
 * This is also called "extended" JavaScript format.</li>
 * </ul>
 * For more information about this modes please see
 * <a href="https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/">
 * https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
 * </a>
 *
 * @since 3.0
 */
public class JsonReader extends AbstractBsonReader {

    private final JsonScanner scanner;
    private JsonToken pushedToken;
    private Object currentValue;

    /**
     * Constructs a new instance with the given string positioned at a JSON object.
     *
     * @param json     A string representation of a JSON object.
     */
    public JsonReader(final String json) {
        this(new JsonScanner(json));
    }

    /**
     * Constructs a new instance with the given {@code Reader} positioned at a JSON object.
     *
     * <p>
     * The application is responsible for closing the {@code Reader}.
     * </p>
     *
     * @param reader A reader representation of a JSON object.
     * @since 3.11
     */
    public JsonReader(final Reader reader) {
        this(new JsonScanner(reader));
    }

    private JsonReader(final JsonScanner scanner) {
        super();
        this.scanner = scanner;
        setContext(new Context(null, BsonContextType.TOP_LEVEL));
    }

    @Override
    protected BsonBinary doReadBinaryData() {
        return (BsonBinary) currentValue;
    }

    @Override
    protected byte doPeekBinarySubType() {
        return doReadBinaryData().getType();
    }

    @Override
    protected int doPeekBinarySize() {
        return doReadBinaryData().getData().length;
    }

    @Override
    protected boolean doReadBoolean() {
        return (Boolean) currentValue;
    }

    //CHECKSTYLE:OFF
    @Override
    public BsonType readBsonType() {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }
        if (getState() == State.INITIAL || getState() == State.DONE || getState() == State.SCOPE_DOCUMENT) {
            // in JSON the top level value can be of any type so fall through
            setState(State.TYPE);
        }
        if (getState() != State.TYPE) {
            throwInvalidState("readBSONType", State.TYPE);
        }

        if (getContext().getContextType() == BsonContextType.DOCUMENT) {
            JsonToken nameToken = popToken();
            switch (nameToken.getType()) {
                case STRING:
                case UNQUOTED_STRING:
                    setCurrentName(nameToken.getValue(String.class));
                    break;
                case END_OBJECT:
                    setState(State.END_OF_DOCUMENT);
                    return BsonType.END_OF_DOCUMENT;
                default:
                    throw new JsonParseException("JSON reader was expecting a name but found '%s'.", nameToken.getValue());
            }

            JsonToken colonToken = popToken();
            if (colonToken.getType() != JsonTokenType.COLON) {
                throw new JsonParseException("JSON reader was expecting ':' but found '%s'.", colonToken.getValue());
            }
        }

        JsonToken token = popToken();
        if (getContext().getContextType() == BsonContextType.ARRAY && token.getType() == JsonTokenType.END_ARRAY) {
            setState(State.END_OF_ARRAY);
            return BsonType.END_OF_DOCUMENT;
        }

        boolean noValueFound = false;
        switch (token.getType()) {
            case BEGIN_ARRAY:
                setCurrentBsonType(BsonType.ARRAY);
                break;
            case BEGIN_OBJECT:
                visitExtendedJSON();
                break;
            case DOUBLE:
                setCurrentBsonType(BsonType.DOUBLE);
                currentValue = token.getValue();
                break;
            case END_OF_FILE:
                setCurrentBsonType(BsonType.END_OF_DOCUMENT);
                break;
            case INT32:
                setCurrentBsonType(BsonType.INT32);
                currentValue = token.getValue();
                break;
            case INT64:
                setCurrentBsonType(BsonType.INT64);
                currentValue = token.getValue();
                break;
            case REGULAR_EXPRESSION:
                setCurrentBsonType(BsonType.REGULAR_EXPRESSION);
                currentValue = token.getValue();
                break;
            case STRING:
                setCurrentBsonType(BsonType.STRING);
                currentValue = token.getValue();
                break;
            case UNQUOTED_STRING:
                String value = token.getValue(String.class);

                if ("false".equals(value) || "true".equals(value)) {
                    setCurrentBsonType(BsonType.BOOLEAN);
                    currentValue = Boolean.parseBoolean(value);
                } else if ("Infinity".equals(value)) {
                    setCurrentBsonType(BsonType.DOUBLE);
                    currentValue = Double.POSITIVE_INFINITY;
                } else if ("NaN".equals(value)) {
                    setCurrentBsonType(BsonType.DOUBLE);
                    currentValue = Double.NaN;
                } else if ("null".equals(value)) {
                    setCurrentBsonType(BsonType.NULL);
                } else if ("undefined".equals(value)) {
                    setCurrentBsonType(BsonType.UNDEFINED);
                } else if ("MinKey".equals(value)) {
                    visitEmptyConstructor();
                    setCurrentBsonType(BsonType.MIN_KEY);
                    currentValue = new MinKey();
                } else if ("MaxKey".equals(value)) {
                    visitEmptyConstructor();
                    setCurrentBsonType(BsonType.MAX_KEY);
                    currentValue = new MaxKey();
                } else if ("BinData".equals(value)) {
                    setCurrentBsonType(BsonType.BINARY);
                    currentValue = visitBinDataConstructor();
                } else if ("Date".equals(value)) {
                    currentValue = visitDateTimeConstructorWithOutNew();
                    setCurrentBsonType(BsonType.STRING);
                } else if ("HexData".equals(value)) {
                    setCurrentBsonType(BsonType.BINARY);
                    currentValue = visitHexDataConstructor();
                } else if ("ISODate".equals(value)) {
                    setCurrentBsonType(BsonType.DATE_TIME);
                    currentValue = visitISODateTimeConstructor();
                } else if ("NumberInt".equals(value)) {
                    setCurrentBsonType(BsonType.INT32);
                    currentValue = visitNumberIntConstructor();
                } else if ("NumberLong".equals(value)) {
                    setCurrentBsonType(BsonType.INT64);
                    currentValue = visitNumberLongConstructor();
                } else if ("NumberDecimal".equals(value)) {
                    setCurrentBsonType(BsonType.DECIMAL128);
                    currentValue = visitNumberDecimalConstructor();
                } else if ("ObjectId".equals(value)) {
                    setCurrentBsonType(BsonType.OBJECT_ID);
                    currentValue = visitObjectIdConstructor();
                } else if ("Timestamp".equals(value)) {
                    setCurrentBsonType(BsonType.TIMESTAMP);
                    currentValue = visitTimestampConstructor();
                } else if ("RegExp".equals(value)) {
                    setCurrentBsonType(BsonType.REGULAR_EXPRESSION);
                    currentValue = visitRegularExpressionConstructor();
                } else if ("DBPointer".equals(value)) {
                    setCurrentBsonType(BsonType.DB_POINTER);
                    currentValue = visitDBPointerConstructor();
                } else if ("UUID".equals(value)) {
                    setCurrentBsonType(BsonType.BINARY);
                    currentValue = visitUUIDConstructor();
                } else if ("new".equals(value)) {
                    visitNew();
                } else {
                    noValueFound = true;
                }
                break;
            default:
                noValueFound = true;
                break;
        }
        if (noValueFound) {
            throw new JsonParseException("JSON reader was expecting a value but found '%s'.", token.getValue());
        }

        if (getContext().getContextType() == BsonContextType.ARRAY || getContext().getContextType() == BsonContextType.DOCUMENT) {
            JsonToken commaToken = popToken();
            if (commaToken.getType() != JsonTokenType.COMMA) {
                pushToken(commaToken);
            }
        }

        switch (getContext().getContextType()) {
            case DOCUMENT:
            case SCOPE_DOCUMENT:
            default:
                setState(State.NAME);
                break;
            case ARRAY:
            case JAVASCRIPT_WITH_SCOPE:
            case TOP_LEVEL:
                setState(State.VALUE);
                break;
        }
        return getCurrentBsonType();
    }
    //CHECKSTYLE:ON

    @Override
    public Decimal128 doReadDecimal128() {
        return (Decimal128) currentValue;
    }

    @Override
    protected long doReadDateTime() {
        return (Long) currentValue;
    }

    @Override
    protected double doReadDouble() {
        return (Double) currentValue;
    }

    @Override
    protected void doReadEndArray() {
        setContext(getContext().getParentContext());

        if (getContext().getContextType() == BsonContextType.ARRAY || getContext().getContextType() == BsonContextType.DOCUMENT) {
            JsonToken commaToken = popToken();
            if (commaToken.getType() != JsonTokenType.COMMA) {
                pushToken(commaToken);
            }
        }
    }

    @Override
    protected void doReadEndDocument() {
        setContext(getContext().getParentContext());
        if (getContext() != null && getContext().getContextType() == BsonContextType.SCOPE_DOCUMENT) {
            setContext(getContext().getParentContext()); // JavaScriptWithScope
            verifyToken(JsonTokenType.END_OBJECT); // outermost closing bracket for JavaScriptWithScope
        }

        if (getContext() == null) {
            throw new JsonParseException("Unexpected end of document.");
        }

        if (getContext().getContextType() == BsonContextType.ARRAY || getContext().getContextType() == BsonContextType.DOCUMENT) {
            JsonToken commaToken = popToken();
            if (commaToken.getType() != JsonTokenType.COMMA) {
                pushToken(commaToken);
            }
        }
    }

    @Override
    protected int doReadInt32() {
        return (Integer) currentValue;
    }

    @Override
    protected long doReadInt64() {
        return (Long) currentValue;
    }

    @Override
    protected String doReadJavaScript() {
        return (String) currentValue;
    }

    @Override
    protected String doReadJavaScriptWithScope() {
        return (String) currentValue;
    }

    @Override
    protected void doReadMaxKey() {
    }

    @Override
    protected void doReadMinKey() {
    }

    @Override
    protected void doReadNull() {
    }

    @Override
    protected ObjectId doReadObjectId() {
        return (ObjectId) currentValue;
    }

    @Override
    protected BsonRegularExpression doReadRegularExpression() {
        return (BsonRegularExpression) currentValue;
    }

    @Override
    protected BsonDbPointer doReadDBPointer() {
        return (BsonDbPointer) currentValue;
    }

    @Override
    protected void doReadStartArray() {
        setContext(new Context(getContext(), BsonContextType.ARRAY));
    }

    @Override
    protected void doReadStartDocument() {
        setContext(new Context(getContext(), BsonContextType.DOCUMENT));
    }

    @Override
    protected String doReadString() {
        return (String) currentValue;
    }

    @Override
    protected String doReadSymbol() {
        return (String) currentValue;
    }

    @Override
    protected BsonTimestamp doReadTimestamp() {
        return (BsonTimestamp) currentValue;
    }

    @Override
    protected void doReadUndefined() {
    }

    @Override
    protected void doSkipName() {
    }

    @Override
    protected void doSkipValue() {
        switch (getCurrentBsonType()) {
            case ARRAY:
                readStartArray();
                while (readBsonType() != BsonType.END_OF_DOCUMENT) {
                    skipValue();
                }
                readEndArray();
                break;
            case BINARY:
                readBinaryData();
                break;
            case BOOLEAN:
                readBoolean();
                break;
            case DATE_TIME:
                readDateTime();
                break;
            case DOCUMENT:
                readStartDocument();
                while (readBsonType() != BsonType.END_OF_DOCUMENT) {
                    skipName();
                    skipValue();
                }
                readEndDocument();
                break;
            case DOUBLE:
                readDouble();
                break;
            case INT32:
                readInt32();
                break;
            case INT64:
                readInt64();
                break;
            case DECIMAL128:
                readDecimal128();
                break;
            case JAVASCRIPT:
                readJavaScript();
                break;
            case JAVASCRIPT_WITH_SCOPE:
                readJavaScriptWithScope();
                readStartDocument();
                while (readBsonType() != BsonType.END_OF_DOCUMENT) {
                    skipName();
                    skipValue();
                }
                readEndDocument();
                break;
            case MAX_KEY:
                readMaxKey();
                break;
            case MIN_KEY:
                readMinKey();
                break;
            case NULL:
                readNull();
                break;
            case OBJECT_ID:
                readObjectId();
                break;
            case REGULAR_EXPRESSION:
                readRegularExpression();
                break;
            case STRING:
                readString();
                break;
            case SYMBOL:
                readSymbol();
                break;
            case TIMESTAMP:
                readTimestamp();
                break;
            case UNDEFINED:
                readUndefined();
                break;
            default:
        }
    }

    private JsonToken popToken() {
        if (pushedToken != null) {
            JsonToken token = pushedToken;
            pushedToken = null;
            return token;
        } else {
            return scanner.nextToken();
        }
    }

    private void pushToken(final JsonToken token) {
        if (pushedToken == null) {
            pushedToken = token;
        } else {
            throw new BsonInvalidOperationException("There is already a pending token.");
        }
    }

    private void verifyToken(final JsonTokenType expectedType) {
        JsonToken token = popToken();
        if (expectedType != token.getType()) {
            throw new JsonParseException("JSON reader expected token type '%s' but found '%s'.", expectedType, token.getValue());
        }
    }

    private void verifyToken(final JsonTokenType expectedType, final Object expectedValue) {
        JsonToken token = popToken();
        if (expectedType != token.getType()) {
            throw new JsonParseException("JSON reader expected token type '%s' but found '%s'.", expectedType, token.getValue());
        }
        if (!expectedValue.equals(token.getValue())) {
            throw new JsonParseException("JSON reader expected '%s' but found '%s'.", expectedValue, token.getValue());
        }
    }

    private void verifyString(final String expected) {
        if (expected == null) {
            throw new IllegalArgumentException("Can't be null");
        }

        JsonToken token = popToken();
        JsonTokenType type = token.getType();

        if ((type != JsonTokenType.STRING && type != JsonTokenType.UNQUOTED_STRING) || !expected.equals(token.getValue())) {
            throw new JsonParseException("JSON reader expected '%s' but found '%s'.", expected, token.getValue());
        }
    }

    private void visitNew() {
        JsonToken typeToken = popToken();
        if (typeToken.getType() != JsonTokenType.UNQUOTED_STRING) {
            throw new JsonParseException("JSON reader expected a type name but found '%s'.", typeToken.getValue());
        }

        String value = typeToken.getValue(String.class);

        if ("MinKey".equals(value)) {
            visitEmptyConstructor();
            setCurrentBsonType(BsonType.MIN_KEY);
            currentValue = new MinKey();
        } else if ("MaxKey".equals(value)) {
            visitEmptyConstructor();
            setCurrentBsonType(BsonType.MAX_KEY);
            currentValue = new MaxKey();
        } else if ("BinData".equals(value)) {
            currentValue = visitBinDataConstructor();
            setCurrentBsonType(BsonType.BINARY);
        } else if ("Date".equals(value)) {
            currentValue = visitDateTimeConstructor();
            setCurrentBsonType(BsonType.DATE_TIME);
        } else if ("HexData".equals(value)) {
            currentValue = visitHexDataConstructor();
            setCurrentBsonType(BsonType.BINARY);
        } else if ("ISODate".equals(value)) {
            currentValue = visitISODateTimeConstructor();
            setCurrentBsonType(BsonType.DATE_TIME);
        } else if ("NumberInt".equals(value)) {
            currentValue = visitNumberIntConstructor();
            setCurrentBsonType(BsonType.INT32);
        } else if ("NumberLong".equals(value)) {
            currentValue = visitNumberLongConstructor();
            setCurrentBsonType(BsonType.INT64);
        } else if ("NumberDecimal".equals(value)) {
            currentValue = visitNumberDecimalConstructor();
            setCurrentBsonType(BsonType.DECIMAL128);
        } else if ("ObjectId".equals(value)) {
            currentValue = visitObjectIdConstructor();
            setCurrentBsonType(BsonType.OBJECT_ID);
        } else if ("RegExp".equals(value)) {
            currentValue = visitRegularExpressionConstructor();
            setCurrentBsonType(BsonType.REGULAR_EXPRESSION);
        } else if ("DBPointer".equals(value)) {
            currentValue = visitDBPointerConstructor();
            setCurrentBsonType(BsonType.DB_POINTER);
        } else if ("UUID".equals(value)) {
            currentValue = visitUUIDConstructor();
            setCurrentBsonType(BsonType.BINARY);
        } else {
            throw new JsonParseException("JSON reader expected a type name but found '%s'.", value);
        }
    }

    private void visitExtendedJSON() {
        JsonToken nameToken = popToken();
        String value = nameToken.getValue(String.class);
        JsonTokenType type = nameToken.getType();

        if (type == JsonTokenType.STRING || type == JsonTokenType.UNQUOTED_STRING) {

            if ("$binary".equals(value) || "$type".equals(value)) {
                currentValue = visitBinDataExtendedJson(value);
                if (currentValue != null) {
                    setCurrentBsonType(BsonType.BINARY);
                    return;
                }
            } if ("$uuid".equals(value)) {
                currentValue = visitUuidExtendedJson();
                setCurrentBsonType(BsonType.BINARY);
                return;
            } else if ("$regex".equals(value) || "$options".equals(value)) {
                currentValue = visitRegularExpressionExtendedJson(value);
                if (currentValue != null) {
                    setCurrentBsonType(BsonType.REGULAR_EXPRESSION);
                    return;
                }
            } else if ("$code".equals(value)) {
                visitJavaScriptExtendedJson();
                return;
            } else if ("$date".equals(value)) {
                currentValue = visitDateTimeExtendedJson();
                setCurrentBsonType(BsonType.DATE_TIME);
                return;
            } else if ("$maxKey".equals(value)) {
                currentValue = visitMaxKeyExtendedJson();
                setCurrentBsonType(BsonType.MAX_KEY);
                return;
            } else if ("$minKey".equals(value)) {
                currentValue = visitMinKeyExtendedJson();
                setCurrentBsonType(BsonType.MIN_KEY);
                return;
            } else if ("$oid".equals(value)) {
                currentValue = visitObjectIdExtendedJson();
                setCurrentBsonType(BsonType.OBJECT_ID);
                return;
            } else if ("$regularExpression".equals(value)) {
                currentValue = visitNewRegularExpressionExtendedJson();
                setCurrentBsonType(BsonType.REGULAR_EXPRESSION);
                return;
            } else if ("$symbol".equals(value)) {
                currentValue = visitSymbolExtendedJson();
                setCurrentBsonType(BsonType.SYMBOL);
                return;
            } else if ("$timestamp".equals(value)) {
                currentValue = visitTimestampExtendedJson();
                setCurrentBsonType(BsonType.TIMESTAMP);
                return;
            } else if ("$undefined".equals(value)) {
                currentValue = visitUndefinedExtendedJson();
                setCurrentBsonType(BsonType.UNDEFINED);
                return;
            } else if ("$numberLong".equals(value)) {
                currentValue = visitNumberLongExtendedJson();
                setCurrentBsonType(BsonType.INT64);
                return;
            } else if ("$numberInt".equals(value)) {
                currentValue = visitNumberIntExtendedJson();
                setCurrentBsonType(BsonType.INT32);
                return;
            } else if ("$numberDouble".equals(value)) {
                currentValue = visitNumberDoubleExtendedJson();
                setCurrentBsonType(BsonType.DOUBLE);
                return;
            } else if ("$numberDecimal".equals(value)) {
                currentValue = visitNumberDecimalExtendedJson();
                setCurrentBsonType(BsonType.DECIMAL128);
                return;
            } else if ("$dbPointer".equals(value)) {
                currentValue = visitDbPointerExtendedJson();
                setCurrentBsonType(BsonType.DB_POINTER);
                return;
            }
        }

        pushToken(nameToken);
        setCurrentBsonType(BsonType.DOCUMENT);
    }

    private void visitEmptyConstructor() {
        JsonToken nextToken = popToken();
        if (nextToken.getType() == JsonTokenType.LEFT_PAREN) {
            verifyToken(JsonTokenType.RIGHT_PAREN);
        } else {
            pushToken(nextToken);
        }
    }

    private BsonBinary visitBinDataConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(JsonTokenType.COMMA);
        JsonToken bytesToken = popToken();
        if (bytesToken.getType() != JsonTokenType.UNQUOTED_STRING && bytesToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(JsonTokenType.RIGHT_PAREN);

        byte[] bytes = Base64.getDecoder().decode(bytesToken.getValue(String.class));
        return new BsonBinary(subTypeToken.getValue(Integer.class).byteValue(), bytes);
    }

    private BsonBinary visitUUIDConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        String hexString = readStringFromExtendedJson().replace("-", "");
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return new BsonBinary(BsonBinarySubType.UUID_STANDARD, decodeHex(hexString));
    }

    private BsonRegularExpression visitRegularExpressionConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        String pattern = readStringFromExtendedJson();
        String options = "";
        JsonToken commaToken = popToken();
        if (commaToken.getType() == JsonTokenType.COMMA) {
            options = readStringFromExtendedJson();
        } else {
            pushToken(commaToken);
        }
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return new BsonRegularExpression(pattern, options);
    }

    private ObjectId visitObjectIdConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        ObjectId objectId = new ObjectId(readStringFromExtendedJson());
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return objectId;
    }

    private BsonTimestamp visitTimestampConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken timeToken = popToken();
        int time;
        if (timeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        } else {
            time = timeToken.getValue(Integer.class);
        }
        verifyToken(JsonTokenType.COMMA);
        JsonToken incrementToken = popToken();
        int increment;
        if (incrementToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        } else {
            increment = incrementToken.getValue(Integer.class);
        }

        verifyToken(JsonTokenType.RIGHT_PAREN);
        return new BsonTimestamp(time, increment);
    }

    private BsonDbPointer visitDBPointerConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        String namespace = readStringFromExtendedJson();
        verifyToken(JsonTokenType.COMMA);
        ObjectId id = new ObjectId(readStringFromExtendedJson());
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return new BsonDbPointer(namespace, id);
    }

    private int visitNumberIntConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken valueToken = popToken();
        int value;
        if (valueToken.getType() == JsonTokenType.INT32) {
            value = valueToken.getValue(Integer.class);
        } else if (valueToken.getType() == JsonTokenType.STRING) {
            value = Integer.parseInt(valueToken.getValue(String.class));
        } else {
            throw new JsonParseException("JSON reader expected an integer or a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return value;
    }

    private long visitNumberLongConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken valueToken = popToken();
        long value;
        if (valueToken.getType() == JsonTokenType.INT32 || valueToken.getType() == JsonTokenType.INT64) {
            value = valueToken.getValue(Long.class);
        } else if (valueToken.getType() == JsonTokenType.STRING) {
            value = Long.parseLong(valueToken.getValue(String.class));
        } else {
            throw new JsonParseException("JSON reader expected an integer or a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return value;
    }

    private Decimal128 visitNumberDecimalConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken valueToken = popToken();
        Decimal128 value;
        if (valueToken.getType() == JsonTokenType.INT32 || valueToken.getType() == JsonTokenType.INT64
                    || valueToken.getType() == JsonTokenType.DOUBLE) {
            value = valueToken.getValue(Decimal128.class);
        } else if (valueToken.getType() == JsonTokenType.STRING) {
            value = Decimal128.parse(valueToken.getValue(String.class));
        } else {
            throw new JsonParseException("JSON reader expected a number or a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(JsonTokenType.RIGHT_PAREN);
        return value;
    }

    private long visitISODateTimeConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);

        JsonToken token = popToken();
        if (token.getType() == JsonTokenType.RIGHT_PAREN) {
            return new Date().getTime();
        } else if (token.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", token.getValue());
        }

        verifyToken(JsonTokenType.RIGHT_PAREN);

        String dateTimeString = token.getValue(String.class);

        try {
            return DateTimeFormatter.parse(dateTimeString);
        } catch (DateTimeParseException e) {
            throw new JsonParseException("Failed to parse string as a date: " + dateTimeString, e);
        }
    }

    private BsonBinary visitHexDataConstructor() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(JsonTokenType.COMMA);
        String hex = readStringFromExtendedJson();
        verifyToken(JsonTokenType.RIGHT_PAREN);

        if ((hex.length() & 1) != 0) {
            hex = "0" + hex;
        }

        for (final BsonBinarySubType subType : BsonBinarySubType.values()) {
            if (subType.getValue() == subTypeToken.getValue(Integer.class)) {
                return new BsonBinary(subType, decodeHex(hex));
            }
        }
        return new BsonBinary(decodeHex(hex));
    }

    private long visitDateTimeConstructor() {
        DateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.ENGLISH);

        verifyToken(JsonTokenType.LEFT_PAREN);

        JsonToken token = popToken();
        if (token.getType() == JsonTokenType.RIGHT_PAREN) {
            return new Date().getTime();
        } else if (token.getType() == JsonTokenType.STRING) {
            verifyToken(JsonTokenType.RIGHT_PAREN);
            String s = token.getValue(String.class);
            ParsePosition pos = new ParsePosition(0);
            Date dateTime = format.parse(s, pos);
            if (dateTime != null && pos.getIndex() == s.length()) {
                return dateTime.getTime();
            } else {
                throw new JsonParseException("JSON reader expected a date in 'EEE MMM dd yyyy HH:mm:ss z' format but found '%s'.", s);
            }

        } else if (token.getType() == JsonTokenType.INT32 || token.getType() == JsonTokenType.INT64) {
            long[] values = new long[7];
            int pos = 0;
            while (true) {
                if (pos < values.length) {
                    values[pos++] = token.getValue(Long.class);
                }
                token = popToken();
                if (token.getType() == JsonTokenType.RIGHT_PAREN) {
                    break;
                }
                if (token.getType() != JsonTokenType.COMMA) {
                    throw new JsonParseException("JSON reader expected a ',' or a ')' but found '%s'.", token.getValue());
                }
                token = popToken();
                if (token.getType() != JsonTokenType.INT32 && token.getType() != JsonTokenType.INT64) {
                    throw new JsonParseException("JSON reader expected an integer but found '%s'.", token.getValue());
                }
            }
            if (pos == 1) {
                return values[0];
            } else if (pos < 3 || pos > 7) {
                throw new JsonParseException("JSON reader expected 1 or 3-7 integers but found %d.", pos);
            }

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.set(Calendar.YEAR, (int) values[0]);
            calendar.set(Calendar.MONTH, (int) values[1]);
            calendar.set(Calendar.DAY_OF_MONTH, (int) values[2]);
            calendar.set(Calendar.HOUR_OF_DAY, (int) values[3]);
            calendar.set(Calendar.MINUTE, (int) values[4]);
            calendar.set(Calendar.SECOND, (int) values[5]);
            calendar.set(Calendar.MILLISECOND, (int) values[6]);
            return calendar.getTimeInMillis();
        } else {
            throw new JsonParseException("JSON reader expected an integer or a string but found '%s'.", token.getValue());
        }
    }

    private String visitDateTimeConstructorWithOutNew() {
        verifyToken(JsonTokenType.LEFT_PAREN);
        JsonToken token = popToken();
        if (token.getType() != JsonTokenType.RIGHT_PAREN) {
            while (token.getType() != JsonTokenType.END_OF_FILE) {
                token = popToken();
                if (token.getType() == JsonTokenType.RIGHT_PAREN) {
                    break;
                }
            }
            if (token.getType() != JsonTokenType.RIGHT_PAREN) {
                throw new JsonParseException("JSON reader expected a ')' but found '%s'.", token.getValue());
            }
        }

        DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.ENGLISH);
        return df.format(new Date());
    }

    private BsonBinary visitBinDataExtendedJson(final String firstKey) {

        Mark mark = new Mark();

        try {
            verifyToken(JsonTokenType.COLON);

            if (firstKey.equals("$binary")) {
                JsonToken nextToken = popToken();
                if (nextToken.getType() == JsonTokenType.BEGIN_OBJECT) {
                    JsonToken nameToken = popToken();
                    String firstNestedKey = nameToken.getValue(String.class);
                    byte[] data;
                    byte type;
                    if (firstNestedKey.equals("base64")) {
                        verifyToken(JsonTokenType.COLON);
                        data = Base64.getDecoder().decode(readStringFromExtendedJson());
                        verifyToken(JsonTokenType.COMMA);
                        verifyString("subType");
                        verifyToken(JsonTokenType.COLON);
                        type = readBinarySubtypeFromExtendedJson();
                    } else if (firstNestedKey.equals("subType")) {
                        verifyToken(JsonTokenType.COLON);
                        type = readBinarySubtypeFromExtendedJson();
                        verifyToken(JsonTokenType.COMMA);
                        verifyString("base64");
                        verifyToken(JsonTokenType.COLON);
                        data = Base64.getDecoder().decode(readStringFromExtendedJson());
                    } else {
                        throw new JsonParseException("Unexpected key for $binary: " + firstNestedKey);
                    }
                    verifyToken(JsonTokenType.END_OBJECT);
                    verifyToken(JsonTokenType.END_OBJECT);
                    return new BsonBinary(type, data);
                } else {
                    mark.reset();
                    return visitLegacyBinaryExtendedJson(firstKey);
                }
            } else {
                mark.reset();
                return visitLegacyBinaryExtendedJson(firstKey);
            }
        } finally {
            mark.discard();
        }
    }

    private BsonBinary visitLegacyBinaryExtendedJson(final String firstKey) {

        Mark mark = new Mark();

        try {
            verifyToken(JsonTokenType.COLON);

            byte[] data;
            byte type;

            if (firstKey.equals("$binary")) {
                data = Base64.getDecoder().decode(readStringFromExtendedJson());
                verifyToken(JsonTokenType.COMMA);
                verifyString("$type");
                verifyToken(JsonTokenType.COLON);
                type = readBinarySubtypeFromExtendedJson();
            } else {
                type = readBinarySubtypeFromExtendedJson();
                verifyToken(JsonTokenType.COMMA);
                verifyString("$binary");
                verifyToken(JsonTokenType.COLON);
                data = Base64.getDecoder().decode(readStringFromExtendedJson());
            }
            verifyToken(JsonTokenType.END_OBJECT);

            return new BsonBinary(type, data);
        } catch (JsonParseException e) {
            mark.reset();
            return null;
        } catch (NumberFormatException e) {
            mark.reset();
            return null;
        } finally {
            mark.discard();
        }
    }

    private byte readBinarySubtypeFromExtendedJson() {
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.STRING && subTypeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected a string or number but found '%s'.", subTypeToken.getValue());
        }

        if (subTypeToken.getType() == JsonTokenType.STRING) {
            return (byte) Integer.parseInt(subTypeToken.getValue(String.class), 16);
        } else {
            return subTypeToken.getValue(Integer.class).byteValue();
        }
    }

    private long visitDateTimeExtendedJson() {
        long value;
        verifyToken(JsonTokenType.COLON);
        JsonToken valueToken = popToken();
        if (valueToken.getType() == JsonTokenType.BEGIN_OBJECT) {
            JsonToken nameToken = popToken();
            String name = nameToken.getValue(String.class);
            if (!name.equals("$numberLong")) {
                throw new JsonParseException(String.format("JSON reader expected $numberLong within $date, but found %s", name));
            }
            value = visitNumberLongExtendedJson();
            verifyToken(JsonTokenType.END_OBJECT);
        } else {
            if (valueToken.getType() == JsonTokenType.INT32 || valueToken.getType() == JsonTokenType.INT64) {
                value = valueToken.getValue(Long.class);
            } else if (valueToken.getType() == JsonTokenType.STRING) {
                String dateTimeString = valueToken.getValue(String.class);
                try {
                    value = DateTimeFormatter.parse(dateTimeString);
                } catch (DateTimeParseException e) {
                    throw new JsonParseException("Failed to parse string as a date", e);
                }
            } else {
                throw new JsonParseException("JSON reader expected an integer or string but found '%s'.", valueToken.getValue());
            }
            verifyToken(JsonTokenType.END_OBJECT);
        }
        return value;
    }

    private MaxKey visitMaxKeyExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.INT32, 1);
        verifyToken(JsonTokenType.END_OBJECT);
        return new MaxKey();
    }

    private MinKey visitMinKeyExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.INT32, 1);
        verifyToken(JsonTokenType.END_OBJECT);
        return new MinKey();
    }

    private ObjectId visitObjectIdExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        ObjectId objectId = new ObjectId(readStringFromExtendedJson());
        verifyToken(JsonTokenType.END_OBJECT);
        return objectId;
    }

    private BsonRegularExpression visitNewRegularExpressionExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.BEGIN_OBJECT);

        String pattern;
        String options = "";

        String firstKey = readStringFromExtendedJson();
        if (firstKey.equals("pattern")) {
            verifyToken(JsonTokenType.COLON);
            pattern = readStringFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("options");
            verifyToken(JsonTokenType.COLON);
            options = readStringFromExtendedJson();
        } else if (firstKey.equals("options")) {
            verifyToken(JsonTokenType.COLON);
            options = readStringFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("pattern");
            verifyToken(JsonTokenType.COLON);
            pattern = readStringFromExtendedJson();
        } else {
            throw new JsonParseException("Expected 't' and 'i' fields in $timestamp document but found " + firstKey);
        }

        verifyToken(JsonTokenType.END_OBJECT);
        verifyToken(JsonTokenType.END_OBJECT);
        return new BsonRegularExpression(pattern, options);
    }

    private BsonRegularExpression visitRegularExpressionExtendedJson(final String firstKey) {
        Mark extendedJsonMark = new Mark();

        try {
            verifyToken(JsonTokenType.COLON);

            String pattern;
            String options = "";
            if (firstKey.equals("$regex")) {
                pattern = readStringFromExtendedJson();
                verifyToken(JsonTokenType.COMMA);
                verifyString("$options");
                verifyToken(JsonTokenType.COLON);
                options = readStringFromExtendedJson();
            } else {
                options = readStringFromExtendedJson();
                verifyToken(JsonTokenType.COMMA);
                verifyString("$regex");
                verifyToken(JsonTokenType.COLON);
                pattern = readStringFromExtendedJson();
            }
            verifyToken(JsonTokenType.END_OBJECT);
            return new BsonRegularExpression(pattern, options);
        } catch (JsonParseException e) {
            extendedJsonMark.reset();
            return null;
        } finally {
            extendedJsonMark.discard();
        }
    }

    private String readStringFromExtendedJson() {
        JsonToken patternToken = popToken();
        if (patternToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", patternToken.getValue());
        }
        return patternToken.getValue(String.class);
    }


    private String visitSymbolExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        String symbol = readStringFromExtendedJson();
        verifyToken(JsonTokenType.END_OBJECT);
        return symbol;
    }

    private BsonTimestamp visitTimestampExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.BEGIN_OBJECT);

        int time;
        int increment;

        String firstKey = readStringFromExtendedJson();
        if (firstKey.equals("t")) {
            verifyToken(JsonTokenType.COLON);
            time = readIntFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("i");
            verifyToken(JsonTokenType.COLON);
            increment = readIntFromExtendedJson();
        } else if (firstKey.equals("i")) {
            verifyToken(JsonTokenType.COLON);
            increment = readIntFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("t");
            verifyToken(JsonTokenType.COLON);
            time = readIntFromExtendedJson();
        } else {
            throw new JsonParseException("Expected 't' and 'i' fields in $timestamp document but found " + firstKey);
        }

        verifyToken(JsonTokenType.END_OBJECT);
        verifyToken(JsonTokenType.END_OBJECT);
        return new BsonTimestamp(time, increment);
    }

    private int readIntFromExtendedJson() {
        JsonToken nextToken = popToken();
        int value;
        if (nextToken.getType() == JsonTokenType.INT32) {
            value = nextToken.getValue(Integer.class);
        } else if (nextToken.getType() == JsonTokenType.INT64) {
            value = nextToken.getValue(Long.class).intValue();
        } else {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", nextToken.getValue());
        }
        return value;
    }

    private BsonBinary visitUuidExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        String uuidString = readStringFromExtendedJson();
        verifyToken(JsonTokenType.END_OBJECT);
        try {
            UuidStringValidator.validate(uuidString);
            return new BsonBinary(UUID.fromString(uuidString));
        } catch (IllegalArgumentException e) {
            throw new JsonParseException(e);
        }
    }

    private void visitJavaScriptExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        String code = readStringFromExtendedJson();
        JsonToken nextToken = popToken();
        switch (nextToken.getType()) {
            case COMMA:
                verifyString("$scope");
                verifyToken(JsonTokenType.COLON);
                setState(State.VALUE);
                currentValue = code;
                setCurrentBsonType(BsonType.JAVASCRIPT_WITH_SCOPE);
                setContext(new Context(getContext(), BsonContextType.SCOPE_DOCUMENT));
                break;
            case END_OBJECT:
                currentValue = code;
                setCurrentBsonType(BsonType.JAVASCRIPT);
                break;
            default:
                throw new JsonParseException("JSON reader expected ',' or '}' but found '%s'.", nextToken);
        }
    }

    private BsonUndefined visitUndefinedExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        JsonToken valueToken = popToken();
        if (!valueToken.getValue(String.class).equals("true")) {
            throw new JsonParseException("JSON reader requires $undefined to have the value of true but found '%s'.",
                                                valueToken.getValue());
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return new BsonUndefined();
    }

    private Long visitNumberLongExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        Long value;
        String longAsString = readStringFromExtendedJson();
        try {
            value = Long.valueOf(longAsString);
        } catch (NumberFormatException e) {
            throw new JsonParseException(format("Exception converting value '%s' to type %s", longAsString, Long.class.getName()), e);
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return value;
    }

    private Integer visitNumberIntExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        Integer value;
        String intAsString = readStringFromExtendedJson();
        try {
            value = Integer.valueOf(intAsString);
        } catch (NumberFormatException e) {
            throw new JsonParseException(format("Exception converting value '%s' to type %s", intAsString, Integer.class.getName()), e);
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return value;
    }

    private Double visitNumberDoubleExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        Double value;
        String doubleAsString = readStringFromExtendedJson();
        try {
            value = Double.valueOf(doubleAsString);
        } catch (NumberFormatException e) {
            throw new JsonParseException(format("Exception converting value '%s' to type %s", doubleAsString, Double.class.getName()), e);
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return value;
    }

    private Decimal128 visitNumberDecimalExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        Decimal128 value;
        String decimal128AsString = readStringFromExtendedJson();
        try {
            value = Decimal128.parse(decimal128AsString);
        } catch (NumberFormatException e) {
            throw new JsonParseException(format("Exception converting value '%s' to type %s", decimal128AsString,
                    Decimal128.class.getName()), e);
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return value;
    }

    private BsonDbPointer visitDbPointerExtendedJson() {
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.BEGIN_OBJECT);

        String ref;
        ObjectId oid;

        String firstKey = readStringFromExtendedJson();
        if (firstKey.equals("$ref")) {
            verifyToken(JsonTokenType.COLON);
            ref = readStringFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("$id");
            oid = readDbPointerIdFromExtendedJson();
            verifyToken(JsonTokenType.END_OBJECT);
        } else if (firstKey.equals("$id")) {
            oid = readDbPointerIdFromExtendedJson();
            verifyToken(JsonTokenType.COMMA);
            verifyString("$ref");
            verifyToken(JsonTokenType.COLON);
            ref = readStringFromExtendedJson();

        } else {
            throw new JsonParseException("Expected $ref and $id fields in $dbPointer document but found " + firstKey);
        }
        verifyToken(JsonTokenType.END_OBJECT);
        return new BsonDbPointer(ref, oid);
    }

    private ObjectId readDbPointerIdFromExtendedJson() {
        ObjectId oid;
        verifyToken(JsonTokenType.COLON);
        verifyToken(JsonTokenType.BEGIN_OBJECT);
        verifyToken(JsonTokenType.STRING, "$oid");
        oid = visitObjectIdExtendedJson();
        return oid;
    }

    @Override
    public BsonReaderMark getMark() {
        return new Mark();
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    /**
     * An implementation of {@code AbstractBsonReader.Mark}.
     */
    protected class Mark extends AbstractBsonReader.Mark {
        private final JsonToken pushedToken;
        private final Object currentValue;
        private final int markPos;

        /**
         * Construct an instance.
         */
        protected Mark() {
            super();
            pushedToken = JsonReader.this.pushedToken;
            currentValue = JsonReader.this.currentValue;
            markPos = JsonReader.this.scanner.mark();
        }

        @Override
        public void reset() {
            super.reset();
            JsonReader.this.pushedToken = pushedToken;
            JsonReader.this.currentValue = currentValue;
            JsonReader.this.scanner.reset(markPos);
            JsonReader.this.setContext(new Context(getParentContext(), getContextType()));
        }

        /**
         * Discard the mark.
         */
        public void discard() {
            JsonReader.this.scanner.discard(markPos);
        }
    }


    /**
     * An implementation of {@code AbstractBsonReader.Context}/
     */
    protected class Context extends AbstractBsonReader.Context {
        /**
         * Construct an instance.
         *
         * @param parentContext the parent context
         * @param contextType the context type
         */
        protected Context(final AbstractBsonReader.Context parentContext, final BsonContextType contextType) {
            super(parentContext, contextType);
        }

        @Override
        protected Context getParentContext() {
            return (Context) super.getParentContext();
        }

        @Override
        protected BsonContextType getContextType() {
            return super.getContextType();
        }
    }

    private static byte[] decodeHex(final String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("A hex string must contain an even number of characters: " + hex);
        }

        byte[] out = new byte[hex.length() / 2];

        for (int i = 0; i < hex.length(); i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("A hex string can only contain the characters 0-9, A-F, a-f: " + hex);
            }

            out[i / 2] = (byte) (high * 16 + low);
        }

        return out;
    }
}

