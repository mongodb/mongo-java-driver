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

import java.util.Iterator;

public class JSONScanner implements Iterator<JSONToken> {

    private final JSONBuffer buffer;
    private boolean closed = false;


    public JSONScanner(JSONBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public JSONToken next() {

        int c = buffer.read();
        while (c != -1 && Character.isWhitespace(c)) {
            c = buffer.read();
        }
        if (c == -1) {
            return new JSONToken(JSONTokenType.END_OF_FILE, "<eof>");
        }

        switch ((char) c) {
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
                return nextStringToken((char) c);
            case '/':
                return nextRegularExpressionToken();
            default:
                if (c == '-' || Character.isDigit(c)) {
                    return nextNumberToken((char) c);
                } else if (c == '$' || c == '_' || Character.isLetter(c)) {
                    return nextUnquotedStringToken();
                } else {
                    buffer.unread(c);
                    throw new IllegalArgumentException("Invalid JSON input");  //TODO correct message
                }
        }
    }

    private JSONToken nextRegularExpressionToken() {
        // opening slash has already been read
        int startPattern = buffer.getPosition() - 1;
        int startOptions = -1;

        RegularExpressionState state = RegularExpressionState.InPattern;
        while (true) {
            int c = buffer.read();
            switch (state) {
                case InPattern:
                    switch (c) {
                        case '/':
                            state = RegularExpressionState.InOptions;
                            startOptions = buffer.getPosition();
                            break;
                        case '\\':
                            state = RegularExpressionState.InEscapeSequence;
                            break;
                        default:
                            state = RegularExpressionState.InPattern;
                            break;
                    }
                    break;
                case InEscapeSequence:
                    state = RegularExpressionState.InPattern;
                    break;
                case InOptions:
                    switch (c) {
                        case 'i':
                        case 'm':
                        case 'x':
                        case 's':
                            state = RegularExpressionState.InOptions;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = RegularExpressionState.Done;
                            break;
                        default:
                            if (Character.isWhitespace((char) c)) {
                                state = RegularExpressionState.Done;
                            } else {
                                state = RegularExpressionState.Invalid;
                            }
                            break;
                    }
                    break;
            }

            switch (state) {
                case Done:
                    buffer.unread(c);
                    RegularExpression regex;
                    if (startOptions < 0){
                        String pattern = buffer.substring(startPattern+1, buffer.getPosition());
                        regex = new RegularExpression(pattern);
                    } else {
                        String pattern = buffer.substring(startPattern+1, startOptions - 1);
                        String options = buffer.substring(startOptions, buffer.getPosition());
                        regex = new RegularExpression(pattern,options);
                    }
                    return new RegularExpressionJSONToken(buffer.substring(startPattern,buffer.getPosition()), regex);
                case Invalid:
                    throw new IllegalArgumentException("Invalid JSON regular expression"); //TODO Correct message;
            }
        }
    }

    private JSONToken nextUnquotedStringToken() {
        int start = buffer.getPosition() - 1;
        int c = buffer.read();
        while (c == '$' || c == '_' || Character.isLetterOrDigit((char) c)) {
            c = buffer.read();
        }
        buffer.unread(c);
        String lexeme = buffer.substring(start, buffer.getPosition());
        return new StringJSONToken(JSONTokenType.UNQUOTED_STRING, lexeme, lexeme);
    }

    private JSONToken nextNumberToken(char firstChar) {

        int c = firstChar;

        int start = buffer.getPosition() - 1;

        NumberState state;

        switch (c) {
            case '-':
                state = NumberState.SawLeadingMinus;
                break;
            case '0':
                state = NumberState.SawLeadingZero;
                break;
            default:
                state = NumberState.SawIntegerDigits;
                break;
        }

        JSONTokenType type = JSONTokenType.INT64;


        while (true) {
            c = buffer.read();
            switch (state) {
                case SawLeadingMinus:
                    switch (c) {
                        case '0':
                            state = NumberState.SawLeadingZero;
                            break;
                        case 'I':
                            state = NumberState.SawMinusI;
                            break;
                        default:
                            if (Character.isDigit((char) c)) {
                                state = NumberState.SawIntegerDigits;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawLeadingZero:
                    switch (c) {
                        case '.':
                            state = NumberState.SawDecimalPoint;
                            break;
                        case 'e':
                        case 'E':
                            state = NumberState.SawExponentLetter;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.Done;
                            break;
                        default:
                            if (Character.isWhitespace((char) c)) {
                                state = NumberState.Done;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawIntegerDigits:
                    switch (c) {
                        case '.':
                            state = NumberState.SawDecimalPoint;
                            break;
                        case 'e':
                        case 'E':
                            state = NumberState.SawExponentLetter;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.Done;
                            break;
                        default:
                            if (Character.isDigit((char) c)) {
                                state = NumberState.SawIntegerDigits;
                            } else if (Character.isWhitespace((char) c)) {
                                state = NumberState.Done;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawDecimalPoint:
                    type = JSONTokenType.DOUBLE;
                    if (Character.isDigit((char) c)) {
                        state = NumberState.SawFractionDigits;
                    } else {
                        state = NumberState.Invalid;
                    }
                    break;
                case SawFractionDigits:
                    switch (c) {
                        case 'e':
                        case 'E':
                            state = NumberState.SawExponentLetter;
                            break;
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.Done;
                            break;
                        default:
                            if (Character.isDigit((char) c)) {
                                state = NumberState.SawFractionDigits;
                            } else if (Character.isWhitespace((char) c)) {
                                state = NumberState.Done;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawExponentLetter:
                    type = JSONTokenType.DOUBLE;
                    switch (c) {
                        case '+':
                        case '-':
                            state = NumberState.SawExponentSign;
                            break;
                        default:
                            if (Character.isDigit((char) c)) {
                                state = NumberState.SawExponentDigits;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawExponentSign:
                    if (Character.isDigit((char) c)) {
                        state = NumberState.SawExponentDigits;
                    } else {
                        state = NumberState.Invalid;
                    }
                    break;
                case SawExponentDigits:
                    switch (c) {
                        case ',':
                        case '}':
                        case ']':
                        case ')':
                        case -1:
                            state = NumberState.Done;
                            break;
                        default:
                            if (Character.isDigit((char) c)) {
                                state = NumberState.SawExponentDigits;
                            } else if (Character.isWhitespace((char) c)) {
                                state = NumberState.Done;
                            } else {
                                state = NumberState.Invalid;
                            }
                            break;
                    }
                    break;
                case SawMinusI:
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
                        type = JSONTokenType.DOUBLE;
                        switch (c) {
                            case ',':
                            case '}':
                            case ']':
                            case ')':
                            case -1:
                                state = NumberState.Done;
                                break;
                            default:
                                if (Character.isWhitespace((char) c)) {
                                    state = NumberState.Done;
                                } else {
                                    state = NumberState.Invalid;
                                }
                                break;
                        }
                    } else {
                        state = NumberState.Invalid;
                    }
                    break;
            }

            switch (state) {
                case Done:
                    buffer.unread(c);
                    String lexeme = buffer.substring(start, buffer.getPosition());
                    if (type == JSONTokenType.DOUBLE) {
                        double value = Double.parseDouble(lexeme);
                        return new DoubleJSONToken(lexeme, value);
                    } else {
                        long value = Long.parseLong(lexeme);
                        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                            return new Int64JSONToken(lexeme, value);
                        } else {
                            return new Int32JSONToken(lexeme, (int) value);
                        }
                    }
                case Invalid:
                    throw new IllegalArgumentException("Invalid JSON number"); //TODO correct message
            }
        }

    }

    private JSONToken nextStringToken(char quoteCharacter) {
        int start = buffer.getPosition() - 1;
        StringBuilder sb = new StringBuilder();

        while (true) {
            int c = buffer.read();
            switch ((char) c) {
                case '\\':
                    c = buffer.read();
                    switch ((char) c) {
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
                            if (c != -1) {
                                throw new IllegalArgumentException(String.format("Invalid escape sequence in JSON string '\\{0}'.", c)); //TODO correct message
                            }
                            break;
                    }
                    break;

                default:
                    if (c == quoteCharacter) {
                        String lexeme = buffer.substring(start, buffer.getPosition());
                        return new StringJSONToken(JSONTokenType.STRING, lexeme, sb.toString());
                    }
                    if (c != -1) {
                        sb.append((char) c);
                    }
            }
            if (c == -1) {
                throw new IllegalArgumentException("End of file in JSON string."); //TODO correct message
            }
        }
    }


    /**
     * The remove operation is not supported by this implementation of
     * <code>Iterator</code>.
     *
     * @throws UnsupportedOperationException if this method is invoked.
     * @see java.util.Iterator
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private enum NumberState {
        SawLeadingMinus,
        SawLeadingZero,
        SawIntegerDigits,
        SawDecimalPoint,
        SawFractionDigits,
        SawExponentLetter,
        SawExponentSign,
        SawExponentDigits,
        SawMinusI,
        Done,
        Invalid
    }

    private enum RegularExpressionState {
        InPattern,
        InEscapeSequence,
        InOptions,
        Done,
        Invalid
    }
}
