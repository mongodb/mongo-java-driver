/*
 * Copyright 2008-2016 MongoDB, Inc.
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
import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonContextType;
import org.bson.BsonDbPointer;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import javax.xml.bind.DatatypeConverter;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;


/**
 * Reads a JSON in one of the following modes:
 * <ul>
 *  <li><em>Strict mode</em> that conforms to the <a href="http://www.json.org/">JSON RFC specifications.</a></li>
 *  <li><em>JavaScript mode</em> that that most JavaScript interpreters can process</li>
 *  <li><em>Shell mode</em> that the <a href="http://docs.mongodb.org/manual/reference/mongo/#mongo">mongo</a> shell can process.
 * This is also called "extended" JavaScript format.</li>
 * </ul>
 * For more information about this modes please see
 * <a href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">
 * http://docs.mongodb.org/manual/reference/mongodb-extended-json/
 * </a>
 *
 * @since 3.0
 */
public class JsonReader extends AbstractBsonReader {

    private final JsonScanner scanner;
    private JsonToken pushedToken;
    private Object currentValue;
    private Mark mark;

    /**
     * Constructs a new instance with the given JSON string.
     *
     * @param json     A string representation of a JSON.
     */
    public JsonReader(final String json) {
        super();
        scanner = new JsonScanner(json);
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
                } else if ("UUID".equals(value)
                           || "GUID".equals(value)
                           || "CSUUID".equals(value)
                           || "CSGUID".equals(value)
                           || "JUUID".equals(value)
                           || "JGUID".equals(value)
                           || "PYUUID".equals(value)
                           || "PYGUID".equals(value)) {
                    setCurrentBsonType(BsonType.BINARY);
                    currentValue = visitUUIDConstructor(value);
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
            verifyToken("}"); // outermost closing bracket for JavaScriptWithScope
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

    private void verifyToken(final Object expected) {
        if (expected == null) {
            throw new IllegalArgumentException("Can't be null");
        }
        JsonToken token = popToken();
        if (!expected.equals(token.getValue())) {
            throw new JsonParseException("JSON reader expected '%s' but found '%s'.", expected, token.getValue());
        }
    }

    private void verifyString(final String expected) {
        if (expected == null) {
            throw new IllegalArgumentException("Can't be null");
        }

        JsonToken token = popToken();
        JsonTokenType type = token.getType();

        if ((type != JsonTokenType.STRING && type != JsonTokenType.UNQUOTED_STRING) && !expected.equals(token.getValue())) {
            throw new JsonParseException("JSON reader expected '%s' but found '%s'.", expected, token.getValue());
        }
    }

    private void visitNew() {
        JsonToken typeToken = popToken();
        if (typeToken.getType() != JsonTokenType.UNQUOTED_STRING) {
            throw new JsonParseException("JSON reader expected a type name but found '%s'.", typeToken.getValue());
        }

        String value = typeToken.getValue(String.class);

        if ("BinData".equals(value)) {
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
        } else if ("UUID".equals(value)
                   || "GUID".equals(value)
                   || "CSUUID".equals(value)
                   || "CSGUID".equals(value)
                   || "JUUID".equals(value)
                   || "JGUID".equals(value)
                   || "PYUUID".equals(value)
                   || "PYGUID".equals(value)) {
            currentValue = visitUUIDConstructor(value);
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
            if ("$binary".equals(value)) {
                currentValue = visitBinDataExtendedJson();
                setCurrentBsonType(BsonType.BINARY);
                return;
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
            } else if ("$regex".equals(value)) {
                currentValue = visitRegularExpressionExtendedJson();
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
            } else if ("$numberDecimal".equals(value)) {
                currentValue = visitNumberDecimalExtendedJson();
                setCurrentBsonType(BsonType.DECIMAL128);
                return;
            }
        }
        pushToken(nameToken);
        setCurrentBsonType(BsonType.DOCUMENT);
    }

    private BsonBinary visitBinDataConstructor() {
        verifyToken("(");
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(",");
        JsonToken bytesToken = popToken();
        if (bytesToken.getType() != JsonTokenType.UNQUOTED_STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");

        byte[] bytes = DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class));
        return new BsonBinary(subTypeToken.getValue(Integer.class).byteValue(), bytes);
    }

    private BsonBinary visitUUIDConstructor(final String uuidConstructorName) {
        //TODO verify information related to https://jira.mongodb.org/browse/SERVER-3168
        verifyToken("(");
        JsonToken bytesToken = popToken();
        if (bytesToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");
        String hexString = bytesToken.getValue(String.class).replaceAll("\\{", "").replaceAll("\\}", "").replaceAll("-", "");
        byte[] bytes = DatatypeConverter.parseHexBinary(hexString);
        BsonBinarySubType subType = BsonBinarySubType.UUID_STANDARD;
        if (!"UUID".equals(uuidConstructorName) || !"GUID".equals(uuidConstructorName)) {
            subType = BsonBinarySubType.UUID_LEGACY;
        }
        return new BsonBinary(subType, bytes);
    }

    private BsonRegularExpression visitRegularExpressionConstructor() {
        verifyToken("(");
        JsonToken patternToken = popToken();
        if (patternToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", patternToken.getValue());
        }
        String options = "";
        JsonToken commaToken = popToken();
        if (commaToken.getType() == JsonTokenType.COMMA) {
            JsonToken optionsToken = popToken();
            if (optionsToken.getType() != JsonTokenType.STRING) {
                throw new JsonParseException("JSON reader expected a string but found '%s'.", optionsToken.getValue());
            }
            options = optionsToken.getValue(String.class);
        } else {
            pushToken(commaToken);
        }
        verifyToken(")");
        return new BsonRegularExpression(patternToken.getValue(String.class), options);
    }

    private ObjectId visitObjectIdConstructor() {
        verifyToken("(");
        JsonToken valueToken = popToken();
        if (valueToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(")");
        return new ObjectId(valueToken.getValue(String.class));
    }

    private BsonTimestamp visitTimestampConstructor() {
        verifyToken("(");
        JsonToken timeToken = popToken();
        int time;
        if (timeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        } else {
            time = timeToken.getValue(Integer.class);
        }
        verifyToken(",");
        JsonToken incrementToken = popToken();
        int increment;
        if (incrementToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        } else {
            increment = incrementToken.getValue(Integer.class);
        }

        verifyToken(")");
        return new BsonTimestamp(time, increment);
    }

    private BsonDbPointer visitDBPointerConstructor() {
        verifyToken("(");
        JsonToken namespaceToken = popToken();
        if (namespaceToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", namespaceToken.getValue());
        }
        verifyToken(",");
        JsonToken idToken = popToken();
        if (namespaceToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", idToken.getValue());
        }
        verifyToken(")");
        return new BsonDbPointer(namespaceToken.getValue(String.class), new ObjectId(idToken.getValue(String.class)));
    }

    private long visitNumberLongConstructor() {
        verifyToken("(");
        JsonToken valueToken = popToken();
        long value;
        if (valueToken.getType() == JsonTokenType.INT32 || valueToken.getType() == JsonTokenType.INT64) {
            value = valueToken.getValue(Long.class);
        } else if (valueToken.getType() == JsonTokenType.STRING) {
            value = Long.parseLong(valueToken.getValue(String.class));
        } else {
            throw new JsonParseException("JSON reader expected an integer or a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(")");
        return value;
    }

    private Decimal128 visitNumberDecimalConstructor() {
        verifyToken("(");
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
        verifyToken(")");
        return value;
    }

    private long visitISODateTimeConstructor() {
        verifyToken("(");

        JsonToken token = popToken();
        if (token.getType() == JsonTokenType.RIGHT_PAREN) {
            return new Date().getTime();
        } else if (token.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", token.getValue());
        }

        verifyToken(")");
        String[] patterns = {"yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ssz", "yyyy-MM-dd'T'HH:mm:ss.SSSz"};

        SimpleDateFormat format = new SimpleDateFormat(patterns[0], Locale.ENGLISH);
        ParsePosition pos = new ParsePosition(0);
        String s = token.getValue(String.class);

        if (s.endsWith("Z")) {
            s = s.substring(0, s.length() - 1) + "GMT-00:00";
        }

        for (final String pattern : patterns) {
            format.applyPattern(pattern);
            format.setLenient(true);
            pos.setIndex(0);

            Date date = format.parse(s, pos);

            if (date != null && pos.getIndex() == s.length()) {
                return date.getTime();
            }
        }
        throw new JsonParseException("Invalid date format.");
    }

    private BsonBinary visitHexDataConstructor() {
        verifyToken("(");
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.INT32) {
            throw new JsonParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(",");
        JsonToken bytesToken = popToken();
        if (bytesToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");

        String hex = bytesToken.getValue(String.class);
        if ((hex.length() & 1) != 0) {
            hex = "0" + hex;
        }

        for (final BsonBinarySubType subType : BsonBinarySubType.values()) {
            if (subType.getValue() == subTypeToken.getValue(Integer.class)) {
                return new BsonBinary(subType, DatatypeConverter.parseHexBinary(hex));
            }
        }
        return new BsonBinary(DatatypeConverter.parseHexBinary(hex));
    }

    private long visitDateTimeConstructor() {
        DateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.ENGLISH);

        verifyToken("(");

        JsonToken token = popToken();
        if (token.getType() == JsonTokenType.RIGHT_PAREN) {
            return new Date().getTime();
        } else if (token.getType() == JsonTokenType.STRING) {
            verifyToken(")");
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
        verifyToken("(");
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

    private BsonBinary visitBinDataExtendedJson() {
        verifyToken(":");
        JsonToken bytesToken = popToken();
        if (bytesToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(",");
        verifyString("$type");
        verifyToken(":");
        JsonToken subTypeToken = popToken();
        if (subTypeToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", subTypeToken.getValue());
        }
        verifyToken("}");

        byte subType = (byte) Integer.parseInt(subTypeToken.getValue(String.class), 16);

        for (final BsonBinarySubType st : BsonBinarySubType.values()) {
            if (st.getValue() == subType) {
                return new BsonBinary(st, DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class)));
            }
        }

        return new BsonBinary(DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class)));
    }

    private long visitDateTimeExtendedJson() {
        verifyToken(":");
        JsonToken valueToken = popToken();
        verifyToken("}");

        if (valueToken.getType() == JsonTokenType.INT32 || valueToken.getType() == JsonTokenType.INT64) {
            return valueToken.getValue(Long.class);
        } else if (valueToken.getType() == JsonTokenType.STRING) {
            String dateTimeString = valueToken.getValue(String.class);
            try {
                return DatatypeConverter.parseDateTime(dateTimeString).getTimeInMillis();
            } catch (IllegalArgumentException e) {
                throw new JsonParseException("JSON reader expected an ISO-8601 date time string but found '%s'.", dateTimeString);
            }
        } else {
            throw new JsonParseException("JSON reader expected an integer or string but found '%s'.", valueToken.getValue());
        }
    }

    private MaxKey visitMaxKeyExtendedJson() {
        verifyToken(":");
        verifyToken(1);
        verifyToken("}");
        return new MaxKey();
    }

    private MinKey visitMinKeyExtendedJson() {
        verifyToken(":");
        verifyToken(1);
        verifyToken("}");
        return new MinKey();
    }

    private ObjectId visitObjectIdExtendedJson() {
        verifyToken(":");
        JsonToken valueToken = popToken();
        if (valueToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", valueToken.getValue());
        }
        verifyToken("}");
        return new ObjectId(valueToken.getValue(String.class));
    }

    private BsonRegularExpression visitRegularExpressionExtendedJson() {
        verifyToken(":");
        JsonToken patternToken = popToken();
        if (patternToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", patternToken.getValue());
        }
        String options = "";
        JsonToken commaToken = popToken();
        if (commaToken.getType() == JsonTokenType.COMMA) {
            verifyString("$options");
            verifyToken(":");
            JsonToken optionsToken = popToken();
            if (optionsToken.getType() != JsonTokenType.STRING) {
                throw new JsonParseException("JSON reader expected a string but found '%s'.", optionsToken.getValue());
            }
            options = optionsToken.getValue(String.class);
        } else {
            pushToken(commaToken);
        }
        verifyToken("}");
        return new BsonRegularExpression(patternToken.getValue(String.class), options);
    }

    private String visitSymbolExtendedJson() {
        verifyToken(":");
        JsonToken nameToken = popToken();
        if (nameToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", nameToken.getValue());
        }
        verifyToken("}");
        return nameToken.getValue(String.class);
    }

    private BsonTimestamp visitTimestampExtendedJson() {
        verifyToken(":");
        verifyToken("{");
        verifyString("t");
        verifyToken(":");

        JsonToken timeToken = popToken();
        int time;
        if (timeToken.getType() == JsonTokenType.INT32) {
            time = timeToken.getValue(Integer.class);
        } else {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        }
        verifyToken(",");
        verifyString("i");
        verifyToken(":");
        JsonToken incrementToken = popToken();
        int increment;
        if (incrementToken.getType() == JsonTokenType.INT32) {
            increment = incrementToken.getValue(Integer.class);
        } else {
            throw new JsonParseException("JSON reader expected an integer but found '%s'.", timeToken.getValue());
        }

        verifyToken("}");
        verifyToken("}");
        return new BsonTimestamp(time, increment);
    }

    private void visitJavaScriptExtendedJson() {
        verifyToken(":");
        JsonToken codeToken = popToken();
        if (codeToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", codeToken.getValue());
        }
        JsonToken nextToken = popToken();
        switch (nextToken.getType()) {
            case COMMA:
                verifyString("$scope");
                verifyToken(":");
                setState(State.VALUE);
                currentValue = codeToken.getValue();
                setCurrentBsonType(BsonType.JAVASCRIPT_WITH_SCOPE);
                setContext(new Context(getContext(), BsonContextType.SCOPE_DOCUMENT));
                break;
            case END_OBJECT:
                currentValue = codeToken.getValue();
                setCurrentBsonType(BsonType.JAVASCRIPT);
                break;
            default:
                throw new JsonParseException("JSON reader expected ',' or '}' but found '%s'.", codeToken.getValue());
        }
    }

    private BsonUndefined visitUndefinedExtendedJson() {
        verifyToken(":");
        JsonToken nameToken = popToken();
        if (!nameToken.getValue(String.class).equals("true")) {
            throw new JsonParseException("JSON reader requires $undefined to have the value of true but found '%s'.",
                                         nameToken.getValue());
        }
        verifyToken("}");
        return new BsonUndefined();
    }

    private Long visitNumberLongExtendedJson() {
        verifyToken(":");
        JsonToken nameToken = popToken();
        if (nameToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", nameToken.getValue());
        }
        verifyToken("}");
        return nameToken.getValue(Long.class);
    }

    private Decimal128 visitNumberDecimalExtendedJson() {
        verifyToken(":");
        JsonToken nameToken = popToken();
        if (nameToken.getType() != JsonTokenType.STRING) {
            throw new JsonParseException("JSON reader expected a string but found '%s'.", nameToken.getValue());
        }
        verifyToken("}");
        return nameToken.getValue(Decimal128.class);
    }

    @Override
    public void mark() {
        if (mark != null) {
             throw new BSONException("A mark already exists; it needs to be reset before creating a new one");
         }
        mark = new Mark();
    }

    @Override
    public void reset() {
        if (mark == null) {
             throw new BSONException("trying to reset a mark before creating it");
         }
        mark.reset();
        mark = null;
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }
    protected class Mark extends AbstractBsonReader.Mark {
        private JsonToken pushedToken;
        private Object currentValue;
        private int position;

        protected Mark() {
            super();
            pushedToken = JsonReader.this.pushedToken;
            currentValue = JsonReader.this.currentValue;
            position = JsonReader.this.scanner.getBufferPosition();
        }

        protected void reset() {
            super.reset();
            JsonReader.this.pushedToken = pushedToken;
            JsonReader.this.currentValue = currentValue;
            JsonReader.this.scanner.setBufferPosition(position);
            JsonReader.this.setContext(new Context(getParentContext(), getContextType()));
        }
    }


    protected class Context extends AbstractBsonReader.Context {
        protected Context(final AbstractBsonReader.Context parentContext, final BsonContextType contextType) {
            super(parentContext, contextType);
        }

        protected Context getParentContext() {
            return (Context) super.getParentContext();
        }

        protected BsonContextType getContextType() {
            return super.getContextType();
        }
    }
}

