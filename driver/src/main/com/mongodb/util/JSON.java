/*
 * Copyright 2008-2017 MongoDB, Inc.
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

package com.mongodb.util;

import org.bson.BSONCallback;

/**
 * Helper methods for JSON serialization and de-serialization
 *
 * @see org.bson.json.JsonReader
 * @see org.bson.json.JsonWriter
 * @see com.mongodb.BasicDBObject#toJson()
 * @see com.mongodb.BasicDBObject#parse(String)
 *
 * @deprecated This class has been superseded by to toJson and parse methods on BasicDBObject
 */
@Deprecated
public class JSON {

    /**
     * <p>Serializes an object into its JSON form.</p>
     *
     * <p>This method delegates serialization to {@code JSONSerializers.getLegacy}</p>
     *
     * @param object object to serialize
     * @return String containing JSON form of the object
     * @see JSONSerializers#getLegacy()
     */
    public static String serialize(final Object object) {
        StringBuilder buf = new StringBuilder();
        serialize(object, buf);
        return buf.toString();
    }

    /**
     * <p>Serializes an object into its JSON form.</p>
     *
     * <p>This method delegates serialization to {@code JSONSerializers.getLegacy}</p>
     *
     * @param object   object to serialize
     * @param buf StringBuilder containing the JSON representation under construction
     * @see JSONSerializers#getLegacy()
     */
    public static void serialize(final Object object, final StringBuilder buf) {
        JSONSerializers.getLegacy().serialize(object, buf);
    }

    /**
     * <p>Parses a JSON string and returns a corresponding Java object. The returned value is either a {@link com.mongodb.DBObject DBObject}
     * (if the string is a JSON object or array), or a boxed primitive value according to the following mapping:</p>
     * <ul>
     *     <li>{@code java.lang.Boolean} for {@code true} or {@code false}</li>
     *     <li>{@code java.lang.Integer} for integers between Integer.MIN_VALUE and Integer.MAX_VALUE</li>
     *     <li>{@code java.lang.Long} for integers outside of this range</li>
     *     <li>{@code java.lang.Double} for floating point numbers</li>
     * </ul>
     * If the parameter is a string that contains a single-quoted or double-quoted string, it is returned as an unquoted {@code
     * java.lang.String}. Parses a JSON string representing a JSON value
     *
     * @param jsonString the string to parse
     * @return a Java object representing the JSON data
     * @throws JSONParseException if jsonString is not valid JSON
     */
    public static Object parse(final String jsonString) {
        return parse(jsonString, null);
    }

    /**
     * Parses a JSON string and constructs a corresponding Java object by calling the methods of a {@link org.bson.BSONCallback
     * BSONCallback} during parsing. If the callback {@code c} is null, this method is equivalent to {@link JSON#parse(String)
     * parse(String)}.
     *
     * @param s the string to parse
     * @param c the BSONCallback to call during parsing
     * @return a Java object representing the JSON data
     * @throws JSONParseException if s is not valid JSON
     */
    public static Object parse(final String s, final BSONCallback c) {
        if (s == null || (s.trim()).equals("")) {
            return null;
        }

        JSONParser p = new JSONParser(s, c);
        return p.parse();
    }

    static void string(final StringBuilder a, final String s) {
        a.append("\"");
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (c == '\\') {
                a.append("\\\\");
            } else if (c == '"') {
                a.append("\\\"");
            } else if (c == '\n') {
                a.append("\\n");
            } else if (c == '\r') {
                a.append("\\r");
            } else if (c == '\t') {
                a.append("\\t");
            } else if (c == '\b') {
                a.append("\\b");
            } else if (c < 32) {
                continue;
            } else {
                a.append(c);
            }
        }
        a.append("\"");
    }
}

/**
 * <p>Parser for JSON objects.</p>
 *
 * <p>Supports all types described at www.json.org, except for numbers with "e" or "E" in them.</p>
 */
class JSONParser {

    final String s;
    int pos = 0;
    final BSONCallback _callback;

    JSONParser(final String s) {
        this(s, null);
    }

    @SuppressWarnings("deprecation")
    JSONParser(final String s, final BSONCallback callback) {
        this.s = s;
        _callback = (callback == null) ? new JSONCallback() : callback;
    }


    /**
     * Parse an unknown type.
     *
     * @return Object the next item
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parse() {
        return parse(null);
    }

    /**
     * Parse an unknown type.
     *
     * @return Object the next item
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parse(final String name) {
        Object value = null;
        char current = get();

        switch (current) {
            // null
            case 'n':
                read('n');
                read('u');
                read('l');
                read('l');
                value = null;
                break;
            // NaN
            case 'N':
                read('N');
                read('a');
                read('N');
                value = Double.NaN;
                break;
            // true
            case 't':
                read('t');
                read('r');
                read('u');
                read('e');
                value = true;
                break;
            // false
            case 'f':
                read('f');
                read('a');
                read('l');
                read('s');
                read('e');
                value = false;
                break;
            // string
            case '\'':
            case '\"':
                value = parseString(true);
                break;
            // number
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '+':
            case '-':
                value = parseNumber();
                break;
            // array
            case '[':
                value = parseArray(name);
                break;
            // object
            case '{':
                value = parseObject(name);
                break;
            default:
                throw new JSONParseException(s, pos);
        }
        return value;
    }

    /**
     * Parses an object for the form <i>{}</i> and <i>{ members }</i>.
     *
     * @return DBObject the next object
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parseObject() {
        return parseObject(null);
    }

    /**
     * Parses an object for the form <i>{}</i> and <i>{ members }</i>.
     *
     * @return DBObject the next object
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parseObject(final String name) {
        if (name != null) {
            _callback.objectStart(name);
        } else {
            _callback.objectStart();
        }

        read('{');
        char current = get();
        while (get() != '}') {
            String key = parseString(false);
            read(':');
            Object value = parse(key);
            doCallback(key, value);

            if ((current = get()) == ',') {
                read(',');
            } else {
                break;
            }
        }
        read('}');

        return _callback.objectDone();
    }

    protected void doCallback(final String name, final Object value) {
        if (value == null) {
            _callback.gotNull(name);
        } else if (value instanceof String) {
            _callback.gotString(name, (String) value);
        } else if (value instanceof Boolean) {
            _callback.gotBoolean(name, (Boolean) value);
        } else if (value instanceof Integer) {
            _callback.gotInt(name, (Integer) value);
        } else if (value instanceof Long) {
            _callback.gotLong(name, (Long) value);
        } else if (value instanceof Double) {
            _callback.gotDouble(name, (Double) value);
        }
    }

    /**
     * Read the current character, making sure that it is the expected character. Advances the pointer to the next character.
     *
     * @param ch the character expected
     * @throws JSONParseException if the current character does not match the given character
     */
    public void read(final char ch) {
        if (!check(ch)) {
            throw new JSONParseException(s, pos);
        }
        pos++;
    }

    public char read() {
        if (pos >= s.length()) {
            throw new IllegalStateException("string done");
        }
        return s.charAt(pos++);
    }

    /**
     * Read the current character, making sure that it is a hexidecimal character.
     *
     * @throws JSONParseException if the current character is not a hexidecimal character
     */
    public void readHex() {
        if (pos < s.length()
            && ((s.charAt(pos) >= '0' && s.charAt(pos) <= '9')
                || (s.charAt(pos) >= 'A' && s.charAt(pos) <= 'F')
                || (s.charAt(pos) >= 'a' && s.charAt(pos) <= 'f'))) {
            pos++;
        } else {
            throw new JSONParseException(s, pos);
        }
    }

    /**
     * Checks the current character, making sure that it is the expected character.
     *
     * @param ch the character expected
     * @throws JSONParseException if the current character does not match the given character
     */
    public boolean check(final char ch) {
        return get() == ch;
    }

    /**
     * Advances the position in the string past any whitespace.
     */
    public void skipWS() {
        while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
            pos++;
        }
    }

    /**
     * Returns the current character. Returns -1 if there are no more characters.
     *
     * @return the next character
     */
    public char get() {
        skipWS();
        if (pos < s.length()) {
            return s.charAt(pos);
        }
        return (char) -1;
    }

    /**
     * Parses a string.
     *
     * @return the next string.
     * @throws JSONParseException if invalid JSON is found
     */
    public String parseString(final boolean needQuote) {
        char quot = 0;
        if (check('\'')) {
            quot = '\'';
        } else if (check('\"')) {
            quot = '\"';
        } else if (needQuote) {
            throw new JSONParseException(s, pos);
        }

        char current;

        if (quot > 0) {
            read(quot);
        }
        StringBuilder buf = new StringBuilder();
        int start = pos;
        while (pos < s.length()) {
            current = s.charAt(pos);
            if (quot > 0) {
                if (current == quot) {
                    break;
                }
            } else {
                if (current == ':' || current == ' ') {
                    break;
                }
            }

            if (current == '\\') {
                pos++;

                char x = get();

                char special = 0;

                //CHECKSTYLE:OFF
                switch (x) {
                    case 'u':  // decode unicode
                        buf.append(s.substring(start, pos - 1));
                        pos++;
                        int tempPos = pos;

                        readHex();
                        readHex();
                        readHex();
                        readHex();

                        int codePoint = Integer.parseInt(s.substring(tempPos, tempPos + 4), 16);
                        buf.append((char) codePoint);

                        start = pos;
                        continue;
                    case 'n':
                        special = '\n';
                        break;
                    case 'r':
                        special = '\r';
                        break;
                    case 't':
                        special = '\t';
                        break;
                    case 'b':
                        special = '\b';
                        break;
                    case '"':
                        special = '\"';
                        break;
                    case '\\':
                        special = '\\';
                        break;
                    default:
                        break;
                }
                //CHECKSTYLE:ON

                buf.append(s.substring(start, pos - 1));
                if (special != 0) {
                    pos++;
                    buf.append(special);
                }
                start = pos;
                continue;
            }
            pos++;
        }
        buf.append(s.substring(start, pos));
        if (quot > 0) {
            read(quot);
        }
        return buf.toString();
    }

    /**
     * Parses a number.
     *
     * @return the next number (int or double).
     * @throws JSONParseException if invalid JSON is found
     */
    public Number parseNumber() {

        get();
        int start = this.pos;
        boolean isDouble = false;

        if (check('-') || check('+')) {
            pos++;
        }

        outer:
        while (pos < s.length()) {
            switch (s.charAt(pos)) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    pos++;
                    break;
                case '.':
                    isDouble = true;
                    parseFraction();
                    break;
                case 'e':
                case 'E':
                    isDouble = true;
                    parseExponent();
                    break;
                default:
                    break outer;
            }
        }

        try {
            if (isDouble) {
                return Double.valueOf(s.substring(start, pos));
            }

            Long val = Long.valueOf(s.substring(start, pos));
            if (val <= Integer.MAX_VALUE && val >= Integer.MIN_VALUE) {
                return val.intValue();
            }
            return val;
        } catch (NumberFormatException e) {
            throw new JSONParseException(s, start, e);
        }
    }

    /**
     * Advances the pointed through <i>.digits</i>.
     */
    public void parseFraction() {
        // get past .
        pos++;

        outer:
        while (pos < s.length()) {
            switch (s.charAt(pos)) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    pos++;
                    break;
                case 'e':
                case 'E':
                    parseExponent();
                    break;
                default:
                    break outer;
            }
        }
    }

    /**
     * Advances the pointer through the exponent.
     */
    public void parseExponent() {
        // get past E
        pos++;

        if (check('-') || check('+')) {
            pos++;
        }

        outer:
        while (pos < s.length()) {
            switch (s.charAt(pos)) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    pos++;
                    break;
                default:
                    break outer;
            }
        }
    }

    /**
     * Parses the next array.
     *
     * @return the array
     * @throws JSONParseException if invalid JSON is found
     */
    public Object parseArray() {
        return parseArray(null);
    }

    /**
     * Parses the next array.
     *
     * @return the array
     * @throws JSONParseException if invalid JSON is found
     */
    protected Object parseArray(final String name) {
        if (name != null) {
            _callback.arrayStart(name);
        } else {
            _callback.arrayStart();
        }

        read('[');

        int i = 0;
        char current = get();
        while (current != ']') {
            String elemName = String.valueOf(i++);
            Object elem = parse(elemName);
            doCallback(elemName, elem);

            if ((current = get()) == ',') {
                read(',');
            } else if (current == ']') {
                break;
            } else {
                throw new JSONParseException(s, pos);
            }
        }

        read(']');

        return _callback.arrayDone();
    }

}
