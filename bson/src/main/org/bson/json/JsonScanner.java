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

package org.bson.json;

import org.bson.BsonRegularExpression;

/**
 * Parses the string representation of a JSON object into a set of {@link JsonToken}-derived objects.
 *
 * @since 3.0
 */
class JsonScanner {

    private final JsonBuffer buffer;

    /**
     * @param newPosition the new position of the cursor position in the buffer
     */
    public void setBufferPosition(final int newPosition) {
        buffer.setPosition(newPosition);
    }

    /**
     * @return the current location of the cursor in the buffer
     */
    public int getBufferPosition() {
        return buffer.getPosition();
    }

    /**
     * Constructs a a new {@code JSONScanner} that produces values scanned from specified {@code JSONBuffer}.
     *
     * @param buffer A buffer to be scanned.
     */
    public JsonScanner(final JsonBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Constructs a a new {@code JSONScanner} that produces values scanned from the specified {@code String}.
     *
     * @param json A string representation of a JSON to be scanned.
     */
    public JsonScanner(final String json) {
        this(new JsonBuffer(json));
    }

    /**
     * Finds and returns the next complete token from this scanner. If scanner reached the end of the source, it will return a token with
     * {@code JSONTokenType.END_OF_FILE} type.
     *
     * @return The next token.
     * @throws JsonParseException if source is invalid.
     */
    public JsonToken nextToken() {

        int c = buffer.read();
        while (c != -1 && Character.isWhitespace(c)) {
            c = buffer.read();
        }
        if (c == -1) {
            return new JsonToken(JsonTokenType.END_OF_FILE, "<eof>");
        }

        switch (c) {
            case '{':
                return new JsonToken(JsonTokenType.BEGIN_OBJECT, "{");
            case '}':
                return new JsonToken(JsonTokenType.END_OBJECT, "}");
            case '[':
                return new JsonToken(JsonTokenType.BEGIN_ARRAY, "[");
            case ']':
                return new JsonToken(JsonTokenType.END_ARRAY, "]");
            case '(':
                return new JsonToken(JsonTokenType.LEFT_PAREN, "(");
            case ')':
                return new JsonToken(JsonTokenType.RIGHT_PAREN, ")");
            case ':':
                return new JsonToken(JsonTokenType.COLON, ":");
            case ',':
                return new JsonToken(JsonTokenType.COMMA, ",");
            case '\'':
            case '"':
                return scanString((char) c);
            case '/':
                return scanRegularExpression();
            default:
                if (c == '-' || Character.isDigit(c)) {
                    return scanNumber((char) c);
                } else if (c == '$' || c == '_' || Character.isLetter(c)) {
                    return scanUnquotedString();
                } else {
                    int position = buffer.getPosition();
                    buffer.unread(c);
                    throw new JsonParseException("Invalid JSON input. Position: %d. Character: '%c'.", position, c);
                }
        }
    }

    /**
     * Reads {@code RegularExpressionToken} from source. The following variants of lexemes are possible:
     * <pre>
     *  /pattern/
     *  /\(pattern\)/
     *  /pattern/ims
     * </pre>
     * Options can include 'i','m','x','s'
     *
     * @return The regular expression token.
     * @throws JsonParseException if regular expression representation is not valid.
     */
    private JsonToken scanRegularExpression() {

        int start = buffer.getPosition() - 1;
        int options = -1;

        RegularExpressionState state = RegularExpressionState.IN_PATTERN;
        while (true) {
            int c = buffer.read();
            switch (state) {
                case IN_PATTERN:
                    switch (c) {
                        case -1:
                            state = RegularExpressionState.INVALID;
                            break;
                        case '/':
                            state = RegularExpressionState.IN_OPTIONS;
                            options = buffer.getPosition();
                            break;
                        case '\\':
                            state = RegularExpressionState.IN_ESCAPE_SEQUENCE;
                            break;
                        default:
                            state = RegularExpressionState.IN_PATTERN;
                            break;
                    }
                    break;
                case IN_ESCAPE_SEQUENCE:
                    state = RegularExpressionState.IN_PATTERN;
                    break;
                case IN_OPTIONS:
                    switch (c) {
                        case 'i':
                        case 'm':
                        case 'x':
                        case 's':
                            state = RegularExpressionState.IN_OPTIONS;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = RegularExpressionState.DONE;
                            break;
                        default:
                            if (Character.isWhitespace(c)) {
                                state = RegularExpressionState.DONE;
                            } else {
                                state = RegularExpressionState.INVALID;
                            }
                            break;
                    }
                    break;
                default:
                    break;
            }

            switch (state) {
                case DONE:
                    buffer.unread(c);
                    int end = buffer.getPosition();
                    BsonRegularExpression regex
                        = new BsonRegularExpression(buffer.substring(start + 1, options - 1), buffer.substring(options, end));
                    return new JsonToken(JsonTokenType.REGULAR_EXPRESSION, regex);
                case INVALID:
                    throw new JsonParseException("Invalid JSON regular expression. Position: %d.", buffer.getPosition());
                default:
            }
        }
    }

    /**
     * Reads {@code StringToken} from source.
     *
     * @return The string token.
     */
    private JsonToken scanUnquotedString() {
        int start = buffer.getPosition() - 1;
        int c = buffer.read();
        while (c == '$' || c == '_' || Character.isLetterOrDigit(c)) {
            c = buffer.read();
        }
        buffer.unread(c);
        String lexeme = buffer.substring(start, buffer.getPosition());
        return new JsonToken(JsonTokenType.UNQUOTED_STRING, lexeme);
    }

    /**
     * Reads number token from source. The following variants of lexemes are possible:
     * <pre>
     *  12
     *  123
     *  -0
     *  -345
     *  -0.0
     *  0e1
     *  0e-1
     *  -0e-1
     *  1e12
     *  -Infinity
     * </pre>
     *
     * @return The number token.
     * @throws JsonParseException if number representation is invalid.
     */
    //CHECKSTYLE:OFF
    private JsonToken scanNumber(final char firstChar) {

        int c = firstChar;

        int start = buffer.getPosition() - 1;

        NumberState state;

        switch (c) {
            case '-':
                state = NumberState.SAW_LEADING_MINUS;
                break;
            case '0':
                state = NumberState.SAW_LEADING_ZERO;
                break;
            default:
                state = NumberState.SAW_INTEGER_DIGITS;
                break;
        }

        JsonTokenType type = JsonTokenType.INT64;


        while (true) {
            c = buffer.read();
            switch (state) {
                case SAW_LEADING_MINUS:
                    switch (c) {
                        case '0':
                            state = NumberState.SAW_LEADING_ZERO;
                            break;
                        case 'I':
                            state = NumberState.SAW_MINUS_I;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_INTEGER_DIGITS;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_LEADING_ZERO:
                    switch (c) {
                        case '.':
                            state = NumberState.SAW_DECIMAL_POINT;
                            break;
                        case 'e':
                        case 'E':
                            state = NumberState.SAW_EXPONENT_LETTER;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.DONE;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_INTEGER_DIGITS;
                            } else if (Character.isWhitespace(c)) {
                                state = NumberState.DONE;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_INTEGER_DIGITS:
                    switch (c) {
                        case '.':
                            state = NumberState.SAW_DECIMAL_POINT;
                            break;
                        case 'e':
                        case 'E':
                            state = NumberState.SAW_EXPONENT_LETTER;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.DONE;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_INTEGER_DIGITS;
                            } else if (Character.isWhitespace(c)) {
                                state = NumberState.DONE;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_DECIMAL_POINT:
                    type = JsonTokenType.DOUBLE;
                    if (Character.isDigit(c)) {
                        state = NumberState.SAW_FRACTION_DIGITS;
                    } else {
                        state = NumberState.INVALID;
                    }
                    break;
                case SAW_FRACTION_DIGITS:
                    switch (c) {
                        case 'e':
                        case 'E':
                            state = NumberState.SAW_EXPONENT_LETTER;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.DONE;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_FRACTION_DIGITS;
                            } else if (Character.isWhitespace(c)) {
                                state = NumberState.DONE;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_EXPONENT_LETTER:
                    type = JsonTokenType.DOUBLE;
                    switch (c) {
                        case '+':
                        case '-':
                            state = NumberState.SAW_EXPONENT_SIGN;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_EXPONENT_DIGITS;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_EXPONENT_SIGN:
                    if (Character.isDigit(c)) {
                        state = NumberState.SAW_EXPONENT_DIGITS;
                    } else {
                        state = NumberState.INVALID;
                    }
                    break;
                case SAW_EXPONENT_DIGITS:
                    switch (c) {
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                            state = NumberState.DONE;
                            break;
                        default:
                            if (Character.isDigit(c)) {
                                state = NumberState.SAW_EXPONENT_DIGITS;
                            } else if (Character.isWhitespace(c)) {
                                state = NumberState.DONE;
                            } else {
                                state = NumberState.INVALID;
                            }
                            break;
                    }
                    break;
                case SAW_MINUS_I:
                    boolean sawMinusInfinity = true;
                    char[] nfinity = new char[]{'n', 'f', 'i', 'n', 'i', 't', 'y'};
                    for (int i = 0; i < nfinity.length; i++) {
                        if (c != nfinity[i]) {
                            sawMinusInfinity = false;
                            break;
                        }
                        c = buffer.read();
                    }
                    if (sawMinusInfinity) {
                        type = JsonTokenType.DOUBLE;
                        switch (c) {
                            case ',':
                            case '}':
                            case ']':
                            case ')':
                            case -1:
                                state = NumberState.DONE;
                                break;
                            default:
                                if (Character.isWhitespace(c)) {
                                    state = NumberState.DONE;
                                } else {
                                    state = NumberState.INVALID;
                                }
                                break;
                        }
                    } else {
                        state = NumberState.INVALID;
                    }
                    break;
                default:
            }

            switch (state) {
                case INVALID:
                    throw new JsonParseException("Invalid JSON number");
                case DONE:
                    buffer.unread(c);
                    String lexeme = buffer.substring(start, buffer.getPosition());
                    if (type == JsonTokenType.DOUBLE) {
                        return new JsonToken(JsonTokenType.DOUBLE, Double.parseDouble(lexeme));
                    } else {
                        long value = Long.parseLong(lexeme);
                        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                            return new JsonToken(JsonTokenType.INT64, value);
                        } else {
                            return new JsonToken(JsonTokenType.INT32, (int) value);
                        }
                    }
                default:
            }
        }

    }
    //CHECKSTYLE:ON

    /**
     * Reads {@code StringToken} from source.
     *
     * @return The string token.
     */
    //CHECKSTYLE:OFF
    private JsonToken scanString(final char quoteCharacter) {

        StringBuilder sb = new StringBuilder();

        while (true) {
            int c = buffer.read();
            switch (c) {
                case '\\':
                    c = buffer.read();
                    switch (c) {
                        case '\'':
                            sb.append('\'');
                            break;
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u':
                            int u1 = buffer.read();
                            int u2 = buffer.read();
                            int u3 = buffer.read();
                            int u4 = buffer.read();
                            if (u4 != -1) {
                                String hex = new String(new char[]{(char) u1, (char) u2, (char) u3, (char) u4});
                                sb.append((char) Integer.parseInt(hex, 16));
                            }
                            break;
                        default:
                            throw new JsonParseException("Invalid escape sequence in JSON string '\\%c'.", c);
                    }
                    break;

                default:
                    if (c == quoteCharacter) {
                        return new JsonToken(JsonTokenType.STRING, sb.toString());
                    }
                    if (c != -1) {
                        sb.append((char) c);
                    }
            }
            if (c == -1) {
                throw new JsonParseException("End of file in JSON string.");
            }
        }
    }

    private enum NumberState {
        SAW_LEADING_MINUS,
        SAW_LEADING_ZERO,
        SAW_INTEGER_DIGITS,
        SAW_DECIMAL_POINT,
        SAW_FRACTION_DIGITS,
        SAW_EXPONENT_LETTER,
        SAW_EXPONENT_SIGN,
        SAW_EXPONENT_DIGITS,
        SAW_MINUS_I,
        DONE,
        INVALID
    }

    private enum RegularExpressionState {
        IN_PATTERN,
        IN_ESCAPE_SEQUENCE,
        IN_OPTIONS,
        DONE,
        INVALID
    }
}
