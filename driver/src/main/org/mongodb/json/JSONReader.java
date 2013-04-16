/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.json;


import org.bson.BSONBinarySubType;
import org.bson.BSONContextType;
import org.bson.BSONInvalidOperationException;
import org.bson.BSONReader;
import org.bson.BSONReaderSettings;
import org.bson.BSONType;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

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
 * <p/>
 * <ul>
 * <li><b>Strict mode</b> that conforms to the <a href="http://www.json.org/">JSON RFC specifications.</a></li>
 * <li><b>JavaScript mode</b> that that most JavaScript interpreters can process</li>
 * <li>
 * <b>Shell mode</b> that the <a href="http://docs.mongodb.org/manual/reference/mongo/#mongo">mongo</a> shell can process.
 * This is also called "extended" JavaScript format.
 * </li>
 * </ul>
 * <p/>
 * For more information about this modes please see
 * <a href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">
 * http://docs.mongodb.org/manual/reference/mongodb-extended-json/
 * </a>
 *
 * @since 3.0.0
 */
public class JSONReader extends BSONReader {

    private Context context;
    private final JSONScanner scanner;
    private JSONToken pushedToken;
    private Object currentValue;

    /**
     * Constructs new {@code JSONReader}
     *
     * @param settings The reader settings.
     * @param json     A string representation of a JSON.
     */
    public JSONReader(final BSONReaderSettings settings, final String json) {
        super(settings);
        scanner = new JSONScanner(json);
        context = new Context(null, BSONContextType.TOP_LEVEL);
    }

    /**
     * Constructs new {@code JSONReader} with default settings.
     *
     * @param json A string representation of a JSON.
     */
    public JSONReader(final String json) {
        this(new BSONReaderSettings(), json);
    }

    @Override
    public Binary readBinaryData() {
        checkPreconditions("readBinaryData", BSONType.BINARY);
        setState(getNextState());
        return (Binary) currentValue;
    }

    @Override
    public boolean readBoolean() {
        checkPreconditions("readBoolean", BSONType.BOOLEAN);
        setState(getNextState());
        return (Boolean) currentValue;
    }

    //CHECKSTYLE:OFF
    @Override
    public BSONType readBSONType() {
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

        if (context.getContextType() == BSONContextType.DOCUMENT) {
            final JSONToken nameToken = popToken();
            switch (nameToken.getType()) {
                case STRING:
                case UNQUOTED_STRING:
                    setCurrentName(nameToken.getValue(String.class));
                    break;
                case END_OBJECT:
                    setState(State.END_OF_DOCUMENT);
                    return BSONType.END_OF_DOCUMENT;
                default:
                    throw new JSONParseException("JSON reader was expecting a name but found '%s'.", nameToken.getValue());
            }

            final JSONToken colonToken = popToken();
            if (colonToken.getType() != JSONTokenType.COLON) {
                throw new JSONParseException("JSON reader was expecting ':' but found '%s'.", colonToken.getValue());
            }
        }

        final JSONToken token = popToken();
        if (context.getContextType() == BSONContextType.ARRAY && token.getType() == JSONTokenType.END_ARRAY) {
            setState(State.END_OF_ARRAY);
            return BSONType.END_OF_DOCUMENT;
        }

        boolean noValueFound = false;
        switch (token.getType()) {
            case BEGIN_ARRAY:
                setCurrentBSONType(BSONType.ARRAY);
                break;
            case BEGIN_OBJECT:
                visitExtendedJSON();
                break;
            case DOUBLE:
                setCurrentBSONType(BSONType.DOUBLE);
                currentValue = token.getValue();
                break;
            case END_OF_FILE:
                setCurrentBSONType(BSONType.END_OF_DOCUMENT);
                break;
            case INT32:
                setCurrentBSONType(BSONType.INT32);
                currentValue = token.getValue();
                break;
            case INT64:
                setCurrentBSONType(BSONType.INT64);
                currentValue = token.getValue();
                break;
            case REGULAR_EXPRESSION:
                setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
                currentValue = token.getValue();
                break;
            case STRING:
                setCurrentBSONType(BSONType.STRING);
                currentValue = token.getValue();
                break;
            case UNQUOTED_STRING:
                final String value = token.getValue(String.class);

                if ("false".equals(value) || "true".equals(value)) {
                    setCurrentBSONType(BSONType.BOOLEAN);
                    currentValue = Boolean.parseBoolean(value);
                } else if ("Infinity".equals(value)) {
                    setCurrentBSONType(BSONType.DOUBLE);
                    currentValue = Double.POSITIVE_INFINITY;
                } else if ("NaN".equals(value)) {
                    setCurrentBSONType(BSONType.DOUBLE);
                    currentValue = Double.NaN;
                } else if ("null".equals(value)) {
                    setCurrentBSONType(BSONType.NULL);
                } else if ("undefined".equals(value)) {
                    setCurrentBSONType(BSONType.UNDEFINED);
                } else if ("BinData".equals(value)) {
                    setCurrentBSONType(BSONType.BINARY);
                    currentValue = visitBinDataConstructor();
                } else if ("Date".equals(value)) {
                    setCurrentBSONType(BSONType.DATE_TIME);
                    currentValue = visitDateTimeConstructor(); // withNew = false
                } else if ("HexData".equals(value)) {
                    setCurrentBSONType(BSONType.BINARY);
                    currentValue = visitHexDataConstructor();
                } else if ("ISODate".equals(value)) {
                    setCurrentBSONType(BSONType.DATE_TIME);
                    currentValue = visitISODateTimeConstructor();
                } else if ("NumberLong".equals(value)) {
                    setCurrentBSONType(BSONType.INT64);
                    currentValue = visitNumberLongConstructor();
                } else if ("ObjectId".equals(value)) {
                    setCurrentBSONType(BSONType.OBJECT_ID);
                    currentValue = visitObjectIdConstructor();
                } else if ("RegExp".equals(value)) {
                    setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
                    currentValue = visitRegularExpressionConstructor();
                } else if ("UUID".equals(value)
                        || "GUID".equals(value)
                        || "CSUUID".equals(value)
                        || "CSGUID".equals(value)
                        || "JUUID".equals(value)
                        || "JGUID".equals(value)
                        || "PYUUID".equals(value)
                        || "PYGUID".equals(value)) {
                    setCurrentBSONType(BSONType.BINARY);
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
            throw new JSONParseException("JSON reader was expecting a value but found '%s'.", token.getValue());
        }

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            final JSONToken commaToken = popToken();
            if (commaToken.getType() != JSONTokenType.COMMA) {
                pushToken(commaToken);
            }
        }

        switch (context.getContextType()) {
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
        return getCurrentBSONType();
    }
    //CHECKSTYLE:ON

    @Override
    public long readDateTime() {
        checkPreconditions("readDateTime", BSONType.DATE_TIME);
        setState(getNextState());
        return (Long) currentValue;
    }

    @Override
    public double readDouble() {
        checkPreconditions("readDouble", BSONType.DOUBLE);
        setState(getNextState());
        return (Double) currentValue;
    }

    @Override
    public void readEndArray() {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }

        if (context.getContextType() != BSONContextType.ARRAY) {
            throwInvalidContextType("readEndArray", context.getContextType(), BSONContextType.ARRAY);
        }
        if (getState() == State.TYPE) {
            readBSONType(); // will set state to EndOfArray if at end of array
        }
        if (getState() != State.END_OF_ARRAY) {
            throwInvalidState("ReadEndArray", State.END_OF_ARRAY);
        }

        context = context.popContext();
        switch (context.getContextType()) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new JSONParseException("Unexpected ContextType %s.", context.contextType);
        }

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            final JSONToken commaToken = popToken();
            if (commaToken.getType() != JSONTokenType.COMMA) {
                pushToken(commaToken);
            }
        }
    }

    @Override
    public void readEndDocument() {

        if (context.getContextType() != BSONContextType.DOCUMENT && context.getContextType() != BSONContextType.SCOPE_DOCUMENT) {
            throwInvalidContextType("ReadEndDocument", context.getContextType(), BSONContextType.DOCUMENT, BSONContextType.SCOPE_DOCUMENT);
        }
        if (getState() == State.TYPE) {
            readBSONType(); // will set state to EndOfDocument if at end of document
        }
        if (getState() != State.END_OF_DOCUMENT) {
            throwInvalidState("ReadEndDocument", State.END_OF_DOCUMENT);
        }

        context = context.popContext();
        if (context != null && context.getContextType() == BSONContextType.JAVASCRIPT_WITH_SCOPE) {
            context = context.popContext(); // JavaScriptWithScope
            verifyToken("}"); // outermost closing bracket for JavaScriptWithScope
        }

        if (context == null) {
            throw new JSONParseException("Unexpected end of document.");
        }

        switch (context.getContextType()) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new JSONParseException("Unexpected ContextType %s.", context.contextType);
        }

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            final JSONToken commaToken = popToken();
            if (commaToken.getType() != JSONTokenType.COMMA) {
                pushToken(commaToken);
            }
        }
    }

    @Override
    public int readInt32() {
        checkPreconditions("readInt32", BSONType.INT32);
        setState(getNextState());
        return (Integer) currentValue;
    }

    @Override
    public long readInt64() {
        checkPreconditions("readInt64", BSONType.INT64);
        setState(getNextState());
        return (Long) currentValue;
    }

    @Override
    public String readJavaScript() {
        checkPreconditions("readJavaScript", BSONType.JAVASCRIPT);
        setState(getNextState());
        return (String) currentValue;
    }

    @Override
    public String readJavaScriptWithScope() {
        checkPreconditions("readJavaScriptWithScope", BSONType.JAVASCRIPT_WITH_SCOPE);
        context = new Context(context, BSONContextType.JAVASCRIPT_WITH_SCOPE);
        setState(State.SCOPE_DOCUMENT);
        return (String) currentValue;
    }

    @Override
    public void readMaxKey() {
        checkPreconditions("readMaxKey", BSONType.MAX_KEY);
        setState(getNextState());
    }

    @Override
    public void readMinKey() {
        checkPreconditions("readMinKey", BSONType.MIN_KEY);
        setState(getNextState());
    }

    @Override
    public void readNull() {
        checkPreconditions("readNull", BSONType.NULL);
        setState(getNextState());
    }

    @Override
    public ObjectId readObjectId() {
        checkPreconditions("readObjectId", BSONType.OBJECT_ID);
        setState(getNextState());
        return (ObjectId) currentValue;
    }

    @Override
    public RegularExpression readRegularExpression() {
        checkPreconditions("readRegularExpression", BSONType.REGULAR_EXPRESSION);
        setState(getNextState());
        return (RegularExpression) currentValue;
    }

    @Override
    public void readStartArray() {
        checkPreconditions("readStartArray", BSONType.ARRAY);
        context = new Context(context, BSONContextType.ARRAY);
        setState(State.TYPE);
    }

    @Override
    public void readStartDocument() {
        checkPreconditions("readStartDocument", BSONType.DOCUMENT);
        context = new Context(context, BSONContextType.DOCUMENT);
        setState(State.TYPE);
    }

    @Override
    public String readString() {
        checkPreconditions("readString", BSONType.STRING);
        setState(getNextState());
        return (String) currentValue;
    }

    @Override
    public String readSymbol() {
        checkPreconditions("readSymbol", BSONType.SYMBOL);
        setState(getNextState());
        return (String) currentValue;
    }

    @Override
    public BSONTimestamp readTimestamp() {
        checkPreconditions("readg", BSONType.TIMESTAMP);
        setState(getNextState());
        return (BSONTimestamp) currentValue;
    }

    @Override
    public void readUndefined() {
        checkPreconditions("readUndefined", BSONType.UNDEFINED);
        setState(getNextState());
    }

    @Override
    public void skipName() {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }
        if (getState() != State.NAME) {
            throwInvalidState("skipName", State.NAME);
        }
        setState(State.VALUE);
    }

    @Override
    public void skipValue() {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }
        if (getState() != State.VALUE) {
            throwInvalidState("skipValue", State.VALUE);
        }
        switch (getCurrentBSONType()) {
            case ARRAY:
                readStartArray();
                while (readBSONType() != BSONType.END_OF_DOCUMENT) {
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
                while (readBSONType() != BSONType.END_OF_DOCUMENT) {
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
            case JAVASCRIPT:
                readJavaScript();
                break;
            case JAVASCRIPT_WITH_SCOPE:
                readJavaScriptWithScope();
                readStartDocument();
                while (readBSONType() != BSONType.END_OF_DOCUMENT) {
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

    private JSONToken popToken() {
        if (pushedToken != null) {
            final JSONToken token = pushedToken;
            pushedToken = null;
            return token;
        } else {
            return scanner.nextToken();
        }
    }

    private void pushToken(final JSONToken token) {
        if (pushedToken == null) {
            pushedToken = token;
        } else {
            throw new BSONInvalidOperationException("There is already a pending token.");
        }
    }

    private void verifyToken(final Object expected) {
        if (expected == null) {
            throw new IllegalArgumentException("Can't be null");
        }
        final JSONToken token = popToken();
        if (!expected.equals(token.getValue())) {
            throw new JSONParseException("JSON reader expected '%s' but found '%s'.", expected, token.getValue());
        }
    }

    private void verifyString(final String expected) {
        if (expected == null) {
            throw new IllegalArgumentException("Can't be null");
        }

        final JSONToken token = popToken();
        final JSONTokenType type = token.getType();

        if ((type != JSONTokenType.STRING && type != JSONTokenType.UNQUOTED_STRING) && !expected.equals(token.getValue())) {
            throw new JSONParseException("JSON reader expected '%s' but found '%s'.", expected, token.getValue());
        }
    }

    private void visitNew() {
        final JSONToken typeToken = popToken();
        if (typeToken.getType() != JSONTokenType.UNQUOTED_STRING) {
            throw new JSONParseException("JSON reader expected a type name but found '%s'.", typeToken.getValue());
        }

        final String value = typeToken.getValue(String.class);

        if ("BinData".equals(value)) {
            currentValue = visitBinDataConstructor();
            setCurrentBSONType(BSONType.BINARY);
        } else if ("Date".equals(value)) {
            currentValue = visitDateTimeConstructor();
            setCurrentBSONType(BSONType.DATE_TIME);
        } else if ("HexData".equals(value)) {
            currentValue = visitHexDataConstructor();
            setCurrentBSONType(BSONType.BINARY);
        } else if ("ISODate".equals(value)) {
            currentValue = visitISODateTimeConstructor();
            setCurrentBSONType(BSONType.DATE_TIME);
        } else if ("NumberLong".equals(value)) {
            currentValue = visitNumberLongConstructor();
            setCurrentBSONType(BSONType.INT64);
        } else if ("ObjectId".equals(value)) {
            currentValue = visitObjectIdConstructor();
            setCurrentBSONType(BSONType.OBJECT_ID);
        } else if ("RegExp".equals(value)) {
            currentValue = visitRegularExpressionConstructor();
            setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
        } else if ("UUID".equals(value)
                || "GUID".equals(value)
                || "CSUUID".equals(value)
                || "CSGUID".equals(value)
                || "JUUID".equals(value)
                || "JGUID".equals(value)
                || "PYUUID".equals(value)
                || "PYGUID".equals(value)) {
            currentValue = visitUUIDConstructor(value);
            setCurrentBSONType(BSONType.BINARY);
        } else {
            throw new JSONParseException("JSON reader expected a type name but found '%s'.", value);
        }
    }

    private void visitExtendedJSON() {
        final JSONToken nameToken = popToken();
        final String value = nameToken.getValue(String.class);
        final JSONTokenType type = nameToken.getType();

        if (type == JSONTokenType.STRING || type == JSONTokenType.UNQUOTED_STRING) {
            if ("$binary".equals(value)) {
                currentValue = visitBinDataExtendedJson();
                setCurrentBSONType(BSONType.BINARY);
                return;
            } else if ("$code".equals(value)) {
                visitJavaScriptExtendedJson();
                return;
            } else if ("$date".equals(value)) {
                currentValue = visitDateTimeExtendedJson();
                setCurrentBSONType(BSONType.DATE_TIME);
                return;
            } else if ("$maxkey".equals(value)) {
                currentValue = visitMaxKeyExtendedJson();
                setCurrentBSONType(BSONType.MAX_KEY);
                return;
            } else if ("$minkey".equals(value)) {
                currentValue = visitMinKeyExtendedJson();
                setCurrentBSONType(BSONType.MIN_KEY);
                return;
            } else if ("$oid".equals(value)) {
                currentValue = visitObjectIdExtendedJson();
                setCurrentBSONType(BSONType.OBJECT_ID);
                return;
            } else if ("$regex".equals(value)) {
                currentValue = visitRegularExpressionExtendedJson();
                setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
                return;
            } else if ("$symbol".equals(value)) {
                currentValue = visitSymbolExtendedJson();
                setCurrentBSONType(BSONType.SYMBOL);
                return;
            } else if ("$timestamp".equals(value)) {
                currentValue = visitTimestampExtendedJson();
                setCurrentBSONType(BSONType.TIMESTAMP);
                return;
            }
        }
        pushToken(nameToken);
        setCurrentBSONType(BSONType.DOCUMENT);
    }

    private Binary visitBinDataConstructor() {
        verifyToken("(");
        final JSONToken subTypeToken = popToken();
        if (subTypeToken.getType() != JSONTokenType.INT32) {
            throw new JSONParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(",");
        final JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.UNQUOTED_STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");

        byte[] bytes = DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class));
        BSONBinarySubType subType = BSONBinarySubType.values()[subTypeToken.getValue(Integer.class)];
        return new Binary(subType, bytes);
    }

    private Binary visitUUIDConstructor(final String uuidConstructorName) {
        //TODO verify information related to https://jira.mongodb.org/browse/SERVER-3168
        verifyToken("(");
        final JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");
        final String hexString = bytesToken.getValue(String.class).replaceAll("\\{", "").replaceAll("\\}", "").replaceAll("-", "");
        final byte[] bytes = DatatypeConverter.parseHexBinary(hexString);
        BSONBinarySubType subType = BSONBinarySubType.UuidStandard;
        if (!"UUID".equals(uuidConstructorName) || !"GUID".equals(uuidConstructorName)) {
            subType = BSONBinarySubType.UuidLegacy;
        }
        return new Binary(subType, bytes);
    }

    private RegularExpression visitRegularExpressionConstructor() {
        verifyToken("(");
        final JSONToken patternToken = popToken();
        if (patternToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", patternToken.getValue());
        }
        String options = "";
        final JSONToken commaToken = popToken();
        if (commaToken.getType() == JSONTokenType.COMMA) {
            JSONToken optionsToken = popToken();
            if (optionsToken.getType() != JSONTokenType.STRING) {
                throw new JSONParseException("JSON reader expected a string but found '%s'.", optionsToken.getValue());
            }
            options = optionsToken.getValue(String.class);
        } else {
            pushToken(commaToken);
        }
        verifyToken(")");
        return new RegularExpression(patternToken.getValue(String.class), options);
    }

    private ObjectId visitObjectIdConstructor() {
        verifyToken("(");
        final JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(")");
        return new ObjectId(valueToken.getValue(String.class));
    }

    private long visitNumberLongConstructor() {
        verifyToken("(");
        final JSONToken valueToken = popToken();
        long value;
        if (valueToken.getType() == JSONTokenType.INT32 || valueToken.getType() == JSONTokenType.INT64) {
            value = valueToken.getValue(Long.class);
        } else if (valueToken.getType() == JSONTokenType.STRING) {
            value = Long.parseLong(valueToken.getValue(String.class));
        } else {
            throw new JSONParseException("JSON reader expected an integer or a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(")");
        return value;
    }

    private long visitISODateTimeConstructor() {
        verifyToken("(");
        final JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", valueToken.getValue());
        }
        verifyToken(")");
        final String[] patterns = {"yyyy-MM-dd", "yyyy-MM-dd'T'hh:mm:ssz", "yyyy-MM-dd'T'hh:mm:ss.SSSz"};

        final SimpleDateFormat format = new SimpleDateFormat(patterns[0], Locale.ENGLISH);
        final ParsePosition pos = new ParsePosition(0);
        String s = valueToken.getValue(String.class);

        if (s.endsWith("Z")) {
            s = s.substring(0, s.length() - 1) + "GMT-00:00";
        }

        for (String pattern : patterns) {
            format.applyPattern(pattern);
            format.setLenient(true);
            pos.setIndex(0);

            Date date = format.parse(s, pos);

            if (date != null && pos.getIndex() == s.length()) {
                return date.getTime();
            }
        }
        throw new JSONParseException("Invalid date format.");
    }

    private Binary visitHexDataConstructor() {
        verifyToken("(");
        final JSONToken subTypeToken = popToken();
        if (subTypeToken.getType() != JSONTokenType.INT32) {
            throw new JSONParseException("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getValue());
        }
        verifyToken(",");
        final JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(")");

        String hex = bytesToken.getValue(String.class);
        if ((hex.length() & 1) != 0) {
            hex = "0" + hex;
        }

        for (BSONBinarySubType subType : BSONBinarySubType.values()) {
            if (subType.getValue() == subTypeToken.getValue(Integer.class)) {
                return new Binary(subType, DatatypeConverter.parseHexBinary(hex));
            }
        }
        return new Binary(DatatypeConverter.parseHexBinary(hex));
    }

    private long visitDateTimeConstructor() {
        final DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z");

        verifyToken("(");

        JSONToken token = popToken();
        if (token.getType() == JSONTokenType.RIGHT_PAREN) {
            return new Date().getTime();
        } else if (token.getType() == JSONTokenType.STRING) {
            verifyToken(")");
            final String s = token.getValue(String.class);
            final ParsePosition pos = new ParsePosition(0);
            final Date dateTime = df.parse(s, pos);
            if (dateTime != null && pos.getIndex() == s.length()) {
                return dateTime.getTime();
            } else {
                throw new JSONParseException("JSON reader expected a date in 'EEE MMM dd yyyy HH:mm:ss z' format but found '%s'.", s);
            }

        } else if (token.getType() == JSONTokenType.INT32 || token.getType() == JSONTokenType.INT64) {
            final long[] values = new long[7];
            int pos = 0;
            while (true) {
                if (pos < values.length) {
                    values[pos++] = token.getValue(Long.class);
                }
                token = popToken();
                if (token.getType() == JSONTokenType.RIGHT_PAREN) {
                    break;
                }
                if (token.getType() != JSONTokenType.COMMA) {
                    throw new JSONParseException("JSON reader expected a ',' or a ')' but found '%s'.", token.getValue());
                }
                token = popToken();
                if (token.getType() != JSONTokenType.INT32 && token.getType() != JSONTokenType.INT64) {
                    throw new JSONParseException("JSON reader expected an integer but found '%s'.", token.getValue());
                }
            }
            if (pos == 1) {
                return values[0];
            } else if (pos < 3 && pos > 7) {
                throw new JSONParseException("JSON reader expected 1 or 3-7 integers but found %d.", pos);
            }

            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.set(Calendar.YEAR, (int) values[0]);
            calendar.set(Calendar.MONTH, (int) values[1]);
            calendar.set(Calendar.DAY_OF_MONTH, (int) values[2]);
            calendar.set(Calendar.HOUR_OF_DAY, (int) values[3]);
            calendar.set(Calendar.MINUTE, (int) values[4]);
            calendar.set(Calendar.SECOND, (int) values[5]);
            calendar.set(Calendar.MILLISECOND, (int) values[6]);
            return calendar.getTimeInMillis();
        } else {
            throw new JSONParseException("JSON reader expected an integer or a string but found '%s'.", token.getValue());
        }
    }

    private Binary visitBinDataExtendedJson() {
        verifyToken(":");
        final JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", bytesToken.getValue());
        }
        verifyToken(",");
        verifyString("$type");
        verifyToken(":");
        final JSONToken subTypeToken = popToken();
        if (subTypeToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", subTypeToken.getValue());
        }
        verifyToken("}");

        final byte subType = Byte.parseByte(subTypeToken.getValue(String.class));

        for (BSONBinarySubType st : BSONBinarySubType.values()) {
            if (st.getValue() == subType) {
                return new Binary(st, DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class)));
            }
        }

        return new Binary(DatatypeConverter.parseBase64Binary(bytesToken.getValue(String.class)));
    }

    private long visitDateTimeExtendedJson() {
        verifyToken(":");
        final JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.INT32 && valueToken.getType() != JSONTokenType.INT64) {
            throw new JSONParseException("JSON reader expected an integer but found '%s'.", valueToken.getValue());
        }
        verifyToken("}");
        return valueToken.getValue(Long.class);
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
        final JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", valueToken.getValue());
        }
        verifyToken("}");
        return new ObjectId(valueToken.getValue(String.class));
    }

    private RegularExpression visitRegularExpressionExtendedJson() {
        verifyToken(":");
        final JSONToken patternToken = popToken();
        if (patternToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", patternToken.getValue());
        }
        String options = "";
        final JSONToken commaToken = popToken();
        if (commaToken.getType() == JSONTokenType.COMMA) {
            verifyString("$options");
            verifyToken(":");
            final JSONToken optionsToken = popToken();
            if (optionsToken.getType() != JSONTokenType.STRING) {
                throw new JSONParseException("JSON reader expected a string but found '%s'.", optionsToken.getValue());
            }
            options = optionsToken.getValue(String.class);
        } else {
            pushToken(commaToken);
        }
        verifyToken("}");
        return new RegularExpression(patternToken.getValue(String.class), options);
    }

    private String visitSymbolExtendedJson() {
        verifyToken(":");
        final JSONToken nameToken = popToken();
        if (nameToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", nameToken.getValue());
        }
        verifyToken("}");
        return nameToken.getValue(String.class);
    }

    private BSONTimestamp visitTimestampExtendedJson() {
        verifyToken(":");
        final JSONToken valueToken = popToken();
        final JSONTokenType type = valueToken.getType();
        int value;
        if (type == JSONTokenType.INT32 || type == JSONTokenType.INT64) {
            value = valueToken.getValue(Integer.class);
        } else if (type == JSONTokenType.UNQUOTED_STRING && "NumberLong".equals(valueToken.getValue())) {
            value = (int) visitNumberLongConstructor();
        } else {
            throw new JSONParseException("JSON reader expected an integer but found '%s'.", valueToken.getValue());
        }
        verifyToken("}");
        return new BSONTimestamp(value, 1);
    }

    private void visitJavaScriptExtendedJson() {
        verifyToken(":");
        final JSONToken codeToken = popToken();
        if (codeToken.getType() != JSONTokenType.STRING) {
            throw new JSONParseException("JSON reader expected a string but found '%s'.", codeToken.getValue());
        }
        final JSONToken nextToken = popToken();
        switch (nextToken.getType()) {
            case COMMA:
                verifyString("$scope");
                verifyToken(":");
                setState(State.VALUE);
                currentValue = codeToken.getValue();
                setCurrentBSONType(BSONType.JAVASCRIPT_WITH_SCOPE);
                break;
            case END_OBJECT:
                currentValue = codeToken.getValue();
                setCurrentBSONType(BSONType.JAVASCRIPT);
                break;
            default:
                throw new JSONParseException("JSON reader expected ',' or '}' but found '%s'.", codeToken.getValue());
        }
    }

    private void checkPreconditions(final String methodName, final BSONType type) {
        if (isClosed()) {
            throw new IllegalStateException("This instance has been closed");
        }

        verifyBSONType(methodName, type);
    }

    private State getNextState() {
        switch (context.contextType) {
            case ARRAY:
            case DOCUMENT:
            case SCOPE_DOCUMENT:
                return State.TYPE;
            case TOP_LEVEL:
                return State.DONE;
            default:
                throw new JSONParseException("Unexpected ContextType %s.", context.contextType);
        }
    }

    private static class Context {
        private final Context parentContext;
        private final BSONContextType contextType;

        Context(final Context parentContext, final BSONContextType contextType) {
            this.parentContext = parentContext;
            this.contextType = contextType;
        }

        /**
         * Creates a clone of the context.
         *
         * @return A clone of the context.
         */
        public Context copy() {
            return new Context(parentContext, contextType);
        }

        Context popContext() {
            return parentContext;
        }

        public BSONContextType getContextType() {
            return contextType;
        }
    }
}

