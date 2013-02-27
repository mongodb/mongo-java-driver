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

import org.bson.*;
import org.bson.types.*;

import javax.xml.bind.DatatypeConverter;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static java.lang.String.format;

public class JSONReader extends BSONReader {

    private Context context;
    private final JSONScanner scanner;
    private JSONToken pushedToken;
    private Object currentValue;
    private JSONToken currentToken;

    /**
     * Initializes a new instance of the BsonReader class.
     *
     * @param settings The reader settings.
     * @param json     The json.
     */
    public JSONReader(final BSONReaderSettings settings, final String json) {
        super(settings);

        JSONBuffer buffer = new JSONBuffer(json);
        scanner = new JSONScanner(buffer);
        context = new Context(null, BSONContextType.TOP_LEVEL);

    }

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
            JSONToken nameToken = popToken();
            switch (nameToken.getType()) {
                case STRING:
                case UNQUOTED_STRING:
                    setCurrentName(nameToken.asString());
                    break;
                case END_OBJECT:
                    setState(State.END_OF_DOCUMENT);
                    return BSONType.END_OF_DOCUMENT;
                default:
                    String message = format("JSON reader was expecting a name but found '{0}'.", nameToken.getLexeme());
                    throw new IllegalArgumentException(message);
            }

            JSONToken colonToken = popToken();
            if (colonToken.getType() != JSONTokenType.COLON) {
                String message = format("JSON reader was expecting ':' but found '{0}'.", colonToken.getLexeme());
                throw new IllegalArgumentException(message);
            }
        }

        JSONToken valueToken = popToken();
        if (context.getContextType() == BSONContextType.ARRAY && valueToken.getType() == JSONTokenType.END_ARRAY) {
            setState(State.END_OF_ARRAY);
            return BSONType.END_OF_DOCUMENT;
        }

        boolean noValueFound = false;
        switch (valueToken.getType()) {
            case BEGIN_ARRAY:
                setCurrentBSONType(BSONType.ARRAY);
                break;
            case BEGIN_OBJECT:
                setCurrentBSONType(parseExtendedJSON());
                break;
            case DATE_TIME:
                setCurrentBSONType(BSONType.DATE_TIME);
                currentValue = valueToken.asDateTime();
                break;
            case DOUBLE:
                setCurrentBSONType(BSONType.DOUBLE);
                currentValue = valueToken.asDouble();
                break;
            case END_OF_FILE:
                setCurrentBSONType(BSONType.END_OF_DOCUMENT);
                break;
            case INT32:
                setCurrentBSONType(BSONType.INT32);
                currentValue = valueToken.asInt32();
                break;
            case INT64:
                setCurrentBSONType(BSONType.INT64);
                currentValue = valueToken.asInt64();
                break;
            case OBJECT_ID:
                setCurrentBSONType(BSONType.OBJECT_ID);
                currentValue = valueToken.asObjectId();
                break;
            case REGULAR_EXPRESSION:
                setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
                currentValue = valueToken.asRegularExpression();
                break;
            case STRING:
                setCurrentBSONType(BSONType.STRING);
                currentValue = valueToken.asString();
                break;
            case UNQUOTED_STRING:
                if ("false".equals(valueToken.getLexeme())
                        || "true".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.BOOLEAN);
                    currentValue = Boolean.parseBoolean(valueToken.getLexeme());
                } else if ("Infinity".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.DOUBLE);
                    currentValue = Double.POSITIVE_INFINITY;
                } else if ("NaN".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.DOUBLE);
                    currentValue = Double.NaN;
                } else if ("null".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.NULL);
                } else if ("undefined".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.UNDEFINED);
                } else if ("BinData".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.BINARY);
                    currentValue = parseBinDataConstructor();
                } else if ("Date".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.STRING);
                    currentValue = parseDateTimeConstructor(false); // withNew = false
                } else if ("HexData".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.BINARY);
                    currentValue = parseHexDataConstructor();
                } else if ("ISODate".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.DATE_TIME);
                    currentValue = parseISODateTimeConstructor();
                } else if ("NumberLong".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.INT64);
                    currentValue = parseNumberLongConstructor();
                } else if ("ObjectId".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.OBJECT_ID);
                    currentValue = parseObjectIdConstructor();
                } else if ("RegExp".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.REGULAR_EXPRESSION);
                    currentValue = parseRegularExpressionConstructor();
                } else if ("UUID".equals(valueToken.getLexeme())
                        || "GUID".equals(valueToken.getLexeme())
                        || "CSUUID".equals(valueToken.getLexeme())
                        || "CSGUID".equals(valueToken.getLexeme())
                        || "JUUID".equals(valueToken.getLexeme())
                        || "JGUID".equals(valueToken.getLexeme())
                        || "PYUUID".equals(valueToken.getLexeme())
                        || "PYGUID".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(BSONType.BINARY);
                    currentValue = parseUUIDConstructor(valueToken.getLexeme());
                } else if ("new".equals(valueToken.getLexeme())) {
                    setCurrentBSONType(parseNew());
                } else {
                    noValueFound = true;
                }
                break;
            default:
                noValueFound = true;
                break;
        }
        if (noValueFound) {
            String message = format("JSON reader was expecting a value but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        currentToken = valueToken;

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            JSONToken commaToken = popToken();
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

    private BSONType parseNew() {
        JSONToken typeToken = popToken();
        if (typeToken.getType() != JSONTokenType.UNQUOTED_STRING) {
            String message = format("JSON reader expected a type name but found '%s'.", typeToken.getLexeme());
            throw new IllegalArgumentException(message);
        }

        if ("BinData".equals(typeToken.getLexeme())) {
            currentValue = parseBinDataConstructor();
            return BSONType.BINARY;
        } else if ("Date".equals(typeToken.getLexeme())) {
            currentValue = parseDateTimeConstructor(true);
            return BSONType.DATE_TIME;
        } else if ("HexData".equals(typeToken.getLexeme())) {
            currentValue = parseHexDataConstructor();
            return BSONType.BINARY;
        } else if ("ISODate".equals(typeToken.getLexeme())) {
            currentValue = parseISODateTimeConstructor();
            return BSONType.DATE_TIME;
        } else if ("NumberLong".equals(typeToken.getLexeme())) {
            currentValue = parseNumberLongConstructor();
            return BSONType.INT64;
        } else if ("ObjectId".equals(typeToken.getLexeme())) {
            currentValue = parseObjectIdConstructor();
            return BSONType.OBJECT_ID;
        } else if ("RegExp".equals(typeToken.getLexeme())) {
            currentValue = parseRegularExpressionConstructor();
            return BSONType.REGULAR_EXPRESSION;
        } else if ("UUID".equals(typeToken.getLexeme())
                || "GUID".equals(typeToken.getLexeme())
                || "CSUUID".equals(typeToken.getLexeme())
                || "CSGUID".equals(typeToken.getLexeme())
                || "JUUID".equals(typeToken.getLexeme())
                || "JGUID".equals(typeToken.getLexeme())
                || "PYUUID".equals(typeToken.getLexeme())
                || "PYGUID".equals(typeToken.getLexeme())) {
            currentValue = parseUUIDConstructor(typeToken.getLexeme());
            return BSONType.BINARY;
        } else {
            String message = format("JSON reader expected a type name but found '%s'.", typeToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public long readDateTime() {
        checkPreconditions("readDateTime", BSONType.DATE_TIME);
        setState(getNextState());
        if (currentValue instanceof Date) {
            return ((Date) currentValue).getTime();
        } else {
            return (Long) currentValue;
        }
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
                throw new BSONException(format("Unexpected ContextType %s.", context.contextType));
        }

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            JSONToken commaToken = popToken();
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

        switch (context.getContextType()) {
            case ARRAY:
            case DOCUMENT:
                setState(State.TYPE);
                break;
            case TOP_LEVEL:
                setState(State.DONE);
                break;
            default:
                throw new BSONException(format("Unexpected ContextType %s.", context.contextType));
        }

        if (context.getContextType() == BSONContextType.ARRAY || context.getContextType() == BSONContextType.DOCUMENT) {
            JSONToken commaToken = popToken();
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
        checkPreconditions("readTimestamp", BSONType.TIMESTAMP);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipValue() {
        throw new UnsupportedOperationException();
    }

    private JSONToken popToken() {
        if (pushedToken != null) {
            JSONToken token = pushedToken;
            pushedToken = null;
            return token;
        } else {
            return scanner.next();
        }
    }

    private void pushToken(JSONToken token) {
        if (pushedToken == null) {
            pushedToken = token;
        } else {
            throw new BSONInvalidOperationException("There is already a pending token.");
        }
    }

    private void verifyToken(String expectedLexeme) {
        JSONToken token = popToken();
        if (!token.getLexeme().equals(expectedLexeme)) {
            String message = format("JSON reader expected '%s' but found '%s'.", expectedLexeme, token.getLexeme());
            throw new IllegalArgumentException(message);
        }
    }

    private void verifyString(String expectedString) {
        JSONToken token = popToken();
        if ((token.getType() != JSONTokenType.STRING && token.getType() != JSONTokenType.UNQUOTED_STRING) || !token.asString().equals(expectedString)) {
            String message = format("JSON reader expected '%s' but found '%s'.", expectedString, token.asString());
            throw new IllegalArgumentException(message);
        }
    }

    private BSONType parseExtendedJSON() {
        JSONToken nameToken = popToken();
        if (nameToken.getType() == JSONTokenType.STRING || nameToken.getType() == JSONTokenType.UNQUOTED_STRING) {
            //TODO Refactor this
            if ("$binary".equals(nameToken.asString())) {
                currentValue = parseBinDataExtendedJson();
                return BSONType.BINARY;
            } else if ("$code".equals(nameToken.asString())) {
                BSONType type = parseJavaScriptExtendedJson();
                setCurrentBSONType(type);
                return type;
            } else if ("$date".equals(nameToken.asString())) {
                currentValue = parseDateTimeExtendedJson();
                return BSONType.DATE_TIME;
            } else if ("$maxkey".equals(nameToken.asString())) {
                currentValue = parseMaxKeyExtendedJson();
                return BSONType.MAX_KEY;
            } else if ("$minkey".equals(nameToken.asString())) {
                currentValue = parseMinKeyExtendedJson();
                return BSONType.MIN_KEY;
            } else if ("$oid".equals(nameToken.asString())) {
                currentValue = parseObjectIdExtendedJson();
                return BSONType.OBJECT_ID;
            } else if ("$regex".equals(nameToken.asString())) {
                currentValue = parseRegularExpressionExtendedJson();
                return BSONType.REGULAR_EXPRESSION;
            } else if ("$symbol".equals(nameToken.asString())) {
                currentValue = parseSymbolExtendedJson();
                return BSONType.SYMBOL;
            } else if ("$timestamp".equals(nameToken.asString())) {
                currentValue = parseTimestampExtendedJson();
                return BSONType.TIMESTAMP;
            }
        }
        pushToken(nameToken);
        return BSONType.DOCUMENT;
    }

    private Object parseBinDataConstructor() {
        verifyToken("(");
        JSONToken subTypeToken = popToken();
        if (subTypeToken.getType() != JSONTokenType.INT32) {
            String message = format("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(",");
        JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", bytesToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(")");

        byte[] bytes = new Base64Codec().decode(bytesToken.asString());
        BSONBinarySubType subType = BSONBinarySubType.values()[subTypeToken.asInt32()];
        return new Binary(subType, bytes);
    }

    private Object parseUUIDConstructor(String lexeme) {
        throw new UnsupportedOperationException();
    }

    private Object parseRegularExpressionConstructor() {
        throw new UnsupportedOperationException();
    }

    private Object parseObjectIdConstructor() {
        verifyToken("(");
        JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(")");
        return new ObjectId(valueToken.asString());
    }

    private long parseNumberLongConstructor() {
        verifyToken("(");
        JSONToken valueToken = popToken();
        long value;
        if (valueToken.getType() == JSONTokenType.INT32 || valueToken.getType() == JSONTokenType.INT64) {
            value = valueToken.asInt64();
        } else if (valueToken.getType() == JSONTokenType.STRING) {
            value = Long.parseLong(valueToken.asString());
        } else {
            String message = format("JSON reader expected an integer or a string but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(")");
        return value;
    }

    private long parseISODateTimeConstructor() {
        verifyToken("(");
        JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(")");
        final String[] patterns = {"yyyy-MM-dd", "yyyy-MM-dd'T'hh:mm:ssz", "yyyy-MM-dd'T'hh:mm:ss.SSSz"};

        final SimpleDateFormat format = new SimpleDateFormat(patterns[0], Locale.ENGLISH);
        ParsePosition pos = new ParsePosition(0);
        String str = valueToken.asString();

        if (str.endsWith("Z")) {
            str = str.substring(0, str.length() - 1) + "GMT-00:00";
        }

        for (String pattern : patterns) {
            format.applyPattern(pattern);
            format.setLenient(true);
            pos.setIndex(0);

            Date date = format.parse(str, pos);

            if (date != null && pos.getIndex() == str.length()) {
                return date.getTime();
            }
        }
        throw new IllegalArgumentException("Invalid date format.");
    }

    private Object parseHexDataConstructor() {
        verifyToken("(");
        JSONToken subTypeToken = popToken();
        if (subTypeToken.getType() != JSONTokenType.INT32) {
            String message = format("JSON reader expected a binary subtype but found '%s'.", subTypeToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(",");
        JSONToken bytesToken = popToken();
        if (bytesToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", bytesToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken(")");

        String hex = bytesToken.asString();
        if ((hex.length() & 1) != 0) {
            hex = "0" + hex;
        }

        for (BSONBinarySubType subType : BSONBinarySubType.values()) {
            if (subType.getValue() == subTypeToken.asInt32()) {
                return new Binary(subType, DatatypeConverter.parseHexBinary(hex));
            }
        }
        return new Binary(DatatypeConverter.parseHexBinary(hex));


    }

    private Object parseDateTimeConstructor(boolean withNew) {
        final DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z");

        verifyToken("(");

        // Date when used without "new" behaves differently (JavaScript has some weird parts)
        if (!withNew) {
            verifyToken(")");
            return df.format(new Date());
        }

        JSONToken token = popToken();
        if (")".equals(token.getLexeme())) {
            return new Date();
        } else if (token.getType() == JSONTokenType.STRING) {
            verifyToken(")");
            Date dateTime = df.parse(token.asString(), new ParsePosition(0));
            return dateTime;
        } else if (token.getType() == JSONTokenType.INT32 || token.getType() == JSONTokenType.INT64) {
            long[] values = new long[7];
            int pos = 0;
            while (true) {
                if (pos < values.length) {
                    values[pos++] = token.asInt64();
                }
                token = popToken();
                if (")".equals(token.getLexeme())) {
                    break;
                }
                if (!",".equals(token.getLexeme())) {
                    String message = format("JSON reader expected a ',' or a ')' but found '%s'.", token.getLexeme());
                    throw new IllegalArgumentException(message);
                }
                token = popToken();
                if (token.getType() != JSONTokenType.INT32 && token.getType() != JSONTokenType.INT64) {
                    String message = format("JSON reader expected an integer but found '%s'.", token.getLexeme());
                    throw new IllegalArgumentException(message);
                }
            }
            if (pos == 1) {
                return new Date(values[0]);
            } else if (pos < 3 && pos > 7) {
                String message = format("JSON reader expected 1 or 3-7 integers but found %d.", pos);
                throw new IllegalArgumentException(message);
            }

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.set(Calendar.YEAR, (int) values[0]);
            calendar.set(Calendar.MONTH, (int) values[1]);
            calendar.set(Calendar.DAY_OF_MONTH, (int) values[2]);
            calendar.set(Calendar.HOUR_OF_DAY, (int) values[3]);
            calendar.set(Calendar.MINUTE, (int) values[4]);
            calendar.set(Calendar.SECOND, (int) values[5]);
            calendar.set(Calendar.MILLISECOND, (int) values[6]);
            return calendar.getTime();
        } else {
            String message = format("JSON reader expected an integer or a string but found '{0}'.", token.getLexeme());
            throw new IllegalArgumentException(message);
        }
    }

    private Object parseBinDataExtendedJson() {
        throw new UnsupportedOperationException();
    }

    private Object parseDateTimeExtendedJson() {
        verifyToken(":");
        JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.INT32 && valueToken.getType() != JSONTokenType.INT64) {
            String message = format("JSON reader expected an integer but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken("}");
        return new Date(valueToken.asInt64());
    }

    private MaxKey parseMaxKeyExtendedJson() {
        verifyToken(":");
        verifyToken("1");
        verifyToken("}");
        return new MaxKey();
    }

    private MinKey parseMinKeyExtendedJson() {
        verifyToken(":");
        verifyToken("1");
        verifyToken("}");
        return new MinKey();
    }

    private Object parseObjectIdExtendedJson() {
        verifyToken(":");
        JSONToken valueToken = popToken();
        if (valueToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken("}");
        return new ObjectId(valueToken.asString());
    }

    private Object parseRegularExpressionExtendedJson() {
        verifyToken(":");
        JSONToken patternToken = popToken();
        if (patternToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", patternToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        String options = "";
        JSONToken commaToken = popToken();
        if (commaToken.getType() == JSONTokenType.COMMA) {
            verifyString("$options");
            verifyToken(":");
            JSONToken optionsToken = popToken();
            if (optionsToken.getType() != JSONTokenType.STRING) {
                String message = format("JSON reader expected a string but found '%s'.", optionsToken.getLexeme());
                throw new IllegalArgumentException(message);
            }
            options = optionsToken.asString();
        } else {
            pushToken(commaToken);
        }
        verifyToken("}");
        return new RegularExpression(patternToken.asString(), options);
    }

    private String parseSymbolExtendedJson() {
        verifyToken(":");
        JSONToken nameToken = popToken();
        if (nameToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", nameToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken("}");
        return nameToken.asString(); // will be converted to a BsonSymbol at a higher level
    }

    private Object parseTimestampExtendedJson() {
        verifyToken(":");
        JSONToken valueToken = popToken();
        int value;
        if (valueToken.getType() == JSONTokenType.INT32 || valueToken.getType() == JSONTokenType.INT64) {
            value = valueToken.asInt32();
        } else if (valueToken.getType() == JSONTokenType.UNQUOTED_STRING && "NumberLong".equals(valueToken.getLexeme())) {
            value = (int) parseNumberLongConstructor();
        } else {
            String message = format("JSON reader expected an integer but found '%s'.", valueToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        verifyToken("}");
        return new BSONTimestamp(value, 1);
    }

    private BSONType parseJavaScriptExtendedJson() {
        verifyToken(":");
        JSONToken codeToken = popToken();
        if (codeToken.getType() != JSONTokenType.STRING) {
            String message = format("JSON reader expected a string but found '%s'.", codeToken.getLexeme());
            throw new IllegalArgumentException(message);
        }
        JSONToken nextToken = popToken();
        switch (nextToken.getType()) {
            case COMMA:
                verifyString("$scope");
                verifyToken(":");
                setState(State.VALUE);
                currentValue = codeToken.asString();
                return BSONType.JAVASCRIPT_WITH_SCOPE;
            case END_OBJECT:
                currentValue = codeToken.asString();
                return BSONType.JAVASCRIPT;
            default:
                String message = format("JSON reader expected ',' or '}' but found '%s'.", codeToken.getLexeme());
                throw new IllegalArgumentException(message);
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
                throw new BSONException(format("Unexpected ContextType %s.", context.contextType));
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
