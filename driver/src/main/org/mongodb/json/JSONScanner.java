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

import org.bson.types.RegularExpression;
import org.mongodb.json.tokens.*;

/**
 * Parses the string representation of a JSON object into a set of {@link JSONToken}-derived objects.
 *
 * @since 3.0.0
 */
public class JSONScanner {

    private final JSONBuffer buffer;

    /**
     * Constructs a a new {@code JSONScanner} that produces values scanned from specified {@code JSONBuffer}.
     * @param buffer A buffer to be scanned.
     */
    public JSONScanner(final JSONBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Constructs a a new {@code JSONScanner} that produces values scanned from the specified {@code String}.
     * @param json A string representation of a JSON to be scanned.
     */
    public JSONScanner(final String json) {
        this(new JSONBuffer(json));
    }

    /**
     * Finds and returns the next complete token from this scanner.
     * If scanner reached the end of the source, it will return a token with {@code JSONTokenType.END_OF_FILE} type.
     *
     * @return The next token.
     * @throws JSONParseException if source is invalid.
     */
    public JSONToken nextToken() {

        int c = buffer.read();
        while (c != -1 && Character.isWhitespace(c)) {
            c = buffer.read();
        }
        if (c == -1) {
            return new JSONToken(JSONTokenType.END_OF_FILE, "<eof>");
        }

        switch (c) {
            case '{':
                return new JSONToken(JSONTokenType.BEGIN_OBJECT, "{");
            case '}':
                return new JSONToken(JSONTokenType.END_OBJECT, "}");
            case '[':
                return new JSONToken(JSONTokenType.BEGIN_ARRAY, "[");
            case ']':
                return new JSONToken(JSONTokenType.END_ARRAY, "]");
            case '(':
                return new JSONToken(JSONTokenType.LEFT_PAREN, "(");
            case ')':
                return new JSONToken(JSONTokenType.RIGHT_PAREN, ")");
            case ':':
                return new JSONToken(JSONTokenType.COLON, ":");
            case ',':
                return new JSONToken(JSONTokenType.COMMA, ",");
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
                    final int position = buffer.getPosition();
                    buffer.unread(c);
                    throw new JSONParseException("Invalid JSON input. Position: %d. Character: '%c'.", position, c);
                }
        }
    }

    /**
     * Reads {@code RegularExpressionToken} from source. The following variants of lexemes are possible:
     *
     *<pre>
     *  /pattern/
     *  /\(pattern\)/
     *  /pattern/ims
     *</pre>
     *
     * Options can include 'i','m','x','s'
     *
     * @return The regular expression token.
     *
     * @throws JSONParseException if regular expression representation is not valid.
     */
    private JSONToken scanRegularExpression() {

        final int start = buffer.getPosition() - 1;
        int options = -1;

        RegularExpressionState state = RegularExpressionState.IN_PATTERN;
        while (true) {
            int c = buffer.read();
            switch (state) {
                case IN_PATTERN:
                    switch (c) {
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
            }

            switch (state) {
                case DONE:
                    buffer.unread(c);
                    final int end = buffer.getPosition();
                    final RegularExpression regex = new RegularExpression(buffer.substring(start+1, options-1), buffer.substring(options, end));
                    return new RegularExpressionToken(buffer.substring(start, end), regex);
                case INVALID:
                    throw new JSONParseException("Invalid JSON regular expression. Position: %d.", buffer.getPosition());
            }
        }
    }

    /**
     * Reads {@code StringToken} from source.
     *
     * @return The string token.
     */
    private JSONToken scanUnquotedString() {
        final int start = buffer.getPosition() - 1;
        int c = buffer.read();
        while (c == '$' || c == '_' || Character.isLetterOrDigit(c)) {
            c = buffer.read();
        }
        buffer.unread(c);
        String lexeme = buffer.substring(start, buffer.getPosition());
        return new StringToken(JSONTokenType.UNQUOTED_STRING, lexeme, lexeme);
    }

    /**
     * Reads number token from source. The following variants of lexemes are possible:
     *
     *<pre>
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
     *</pre>
     *
     * @return The number token.
     *
     * @throws JSONParseException if number representation is invalid.
     */
    private JSONToken scanNumber(char firstChar) {

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

        JSONTokenType type = JSONTokenType.INT64;


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
                            if (Character.isWhitespace(c)) {
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
                    type = JSONTokenType.DOUBLE;
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
                    type = JSONTokenType.DOUBLE;
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
                    final char[] nfinity = new char[]{'n', 'f', 'i', 'n', 'i', 't', 'y'};
                    for (int i = 0; i < nfinity.length; i++) {
                        if (c != nfinity[i]) {
                            sawMinusInfinity = false;
                            break;
                        }
                        c = buffer.read();
                    }
                    if (sawMinusInfinity) {
                        type = JSONTokenType.DOUBLE;
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
            }

            switch (state) {
                case DONE:
                    buffer.unread(c);
                    final String lexeme = buffer.substring(start, buffer.getPosition());
                    if (type == JSONTokenType.DOUBLE) {
                        final double value = Double.parseDouble(lexeme);
                        return new DoubleToken(lexeme, value);
                    } else {
                        final long value = Long.parseLong(lexeme);
                        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                            return new Int64Token(lexeme, value);
                        } else {
                            return new Int32Token(lexeme, (int) value);
                        }
                    }
                case INVALID:
                    throw new JSONParseException("Invalid JSON number");
            }
        }

    }

    /**
     * Reads {@code StringToken} from source.
     *
     * @return The string token.
     */
    private JSONToken scanString(char quoteCharacter) {

        final int start = buffer.getPosition() - 1;

        final StringBuilder sb = new StringBuilder();

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
                            throw new JSONParseException("Invalid escape sequence in JSON string '\\%c'.", c);
                    }
                    break;

                default:
                    if (c == quoteCharacter) {
                        String lexeme = buffer.substring(start, buffer.getPosition());
                        return new StringToken(JSONTokenType.STRING, lexeme, sb.toString());
                    }
                    if (c != -1) {
                        sb.append((char) c);
                    }
            }
            if (c == -1) {
                throw new JSONParseException("End of file in JSON string.");
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
