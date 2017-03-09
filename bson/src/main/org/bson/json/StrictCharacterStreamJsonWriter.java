/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.json;

import org.bson.BSONException;
import org.bson.BsonInvalidOperationException;

import java.io.IOException;
import java.io.Writer;

import static org.bson.assertions.Assertions.notNull;

/**
 * A class that writes JSON texts as a character stream via a provided {@link Writer}.
 *
 * @since 3.5
 */
public final class StrictCharacterStreamJsonWriter implements StrictJsonWriter {
    private enum JsonContextType {
        TOP_LEVEL,
        DOCUMENT,
        ARRAY,
    }

    private enum State {
        INITIAL,
        NAME,
        VALUE,
        DONE
    }

    private static class StrictJsonContext {
        private final StrictJsonContext parentContext;
        private final JsonContextType contextType;
        private final String indentation;
        private boolean hasElements;

        StrictJsonContext(final StrictJsonContext parentContext, final JsonContextType contextType, final String indentChars) {
            this.parentContext = parentContext;
            this.contextType = contextType;
            this.indentation = (parentContext == null) ? indentChars : parentContext.indentation + indentChars;
        }
    }

    private final Writer writer;
    private final StrictCharacterStreamJsonWriterSettings settings;
    private StrictJsonContext context = new StrictJsonContext(null, JsonContextType.TOP_LEVEL, "");
    private State state = State.INITIAL;

    /**
     * Construct an instance.
     *
     * @param writer   the writer to write JSON to.
     * @param settings the settings to apply to this writer.
     */
    public StrictCharacterStreamJsonWriter(final Writer writer, final StrictCharacterStreamJsonWriterSettings settings) {
        this.writer = writer;
        this.settings = settings;
    }

    @Override
    public void writeStartObject(final String name) {
        writeName(name);
        writeStartObject();
    }

    @Override
    public void writeStartArray(final String name) {
        writeName(name);
        writeStartArray();
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeBoolean(value);
    }

    @Override
    public void writeNumber(final String name, final String value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeNumber(value);
    }

    @Override
    public void writeString(final String name, final String value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeString(value);
    }

    @Override
    public void writeRaw(final String name, final String value) {
        notNull("name", name);
        notNull("value", value);
        writeName(name);
        writeRaw(value);
    }

    @Override
    public void writeNull(final String name) {
        writeName(name);
        writeNull();
    }

    @Override
    public void writeName(final String name) {
        notNull("name", name);
        checkPreconditions(State.NAME);

        if (context.hasElements) {
            write(",");
        }
        if (settings.isIndent()) {
            write(settings.getNewLineCharacters());
            write(context.indentation);
        } else {
            write(" ");
        }
        writeStringHelper(name);
        write(" : ");

        state = State.VALUE;
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions(State.VALUE);
        preWriteValue();
        write(value ? "true" : "false");
        setNextState();
    }

    @Override
    public void writeNumber(final String value) {
        notNull("value", value);
        checkPreconditions(State.VALUE);
        preWriteValue();
        write(value);
        setNextState();
    }

    @Override
    public void writeString(final String value) {
        notNull("value", value);
        checkPreconditions(State.VALUE);
        preWriteValue();
        writeStringHelper(value);
        setNextState();
    }

    @Override
    public void writeRaw(final String value) {
        notNull("value", value);
        checkPreconditions(State.VALUE);
        preWriteValue();
        write(value);
        setNextState();
    }

    @Override
    public void writeNull() {
        checkPreconditions(State.VALUE);
        preWriteValue();
        write("null");
        setNextState();
    }

    @Override
    public void writeStartObject() {
        checkPreconditions(State.INITIAL, State.VALUE);
        preWriteValue();
        write("{");
        context = new StrictJsonContext(context, JsonContextType.DOCUMENT, settings.getIndentCharacters());
        state = State.NAME;
    }

    @Override
    public void writeStartArray() {
        preWriteValue();
        write("[");
        context = new StrictJsonContext(context, JsonContextType.ARRAY, settings.getIndentCharacters());
        state = State.VALUE;
    }

    @Override
    public void writeEndObject() {
        checkPreconditions(State.NAME);

        if (settings.isIndent() && context.hasElements) {
            write(settings.getNewLineCharacters());
            write(context.parentContext.indentation);
        } else {
            write(" ");
        }
        write("}");
        context = context.parentContext;
        if (context.contextType == JsonContextType.TOP_LEVEL) {
            state = State.DONE;
        } else {
            setNextState();
        }
    }

    @Override
    public void writeEndArray() {
        checkPreconditions(State.VALUE);

        if (context.contextType != JsonContextType.ARRAY) {
            throw new BsonInvalidOperationException("Can't end an array if not in an array");
        }

        write("]");
        context = context.parentContext;
        if (context.contextType == JsonContextType.TOP_LEVEL) {
            state = State.DONE;
        } else {
            setNextState();
        }
    }

    void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    Writer getWriter() {
        return writer;
    }

    private void preWriteValue() {
        if (context.contextType == JsonContextType.ARRAY) {
            if (context.hasElements) {
                write(", ");
            }
        }
        context.hasElements = true;
    }

    private void setNextState() {
        if (context.contextType == JsonContextType.ARRAY) {
            state = State.VALUE;
        } else {
            state = State.NAME;
        }
    }

    private void writeStringHelper(final String str) {
        write('"');
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"':
                    write("\\\"");
                    break;
                case '\\':
                    write("\\\\");
                    break;
                case '\b':
                    write("\\b");
                    break;
                case '\f':
                    write("\\f");
                    break;
                case '\n':
                    write("\\n");
                    break;
                case '\r':
                    write("\\r");
                    break;
                case '\t':
                    write("\\t");
                    break;
                default:
                    switch (Character.getType(c)) {
                        case Character.UPPERCASE_LETTER:
                        case Character.LOWERCASE_LETTER:
                        case Character.TITLECASE_LETTER:
                        case Character.OTHER_LETTER:
                        case Character.DECIMAL_DIGIT_NUMBER:
                        case Character.LETTER_NUMBER:
                        case Character.OTHER_NUMBER:
                        case Character.SPACE_SEPARATOR:
                        case Character.CONNECTOR_PUNCTUATION:
                        case Character.DASH_PUNCTUATION:
                        case Character.START_PUNCTUATION:
                        case Character.END_PUNCTUATION:
                        case Character.INITIAL_QUOTE_PUNCTUATION:
                        case Character.FINAL_QUOTE_PUNCTUATION:
                        case Character.OTHER_PUNCTUATION:
                        case Character.MATH_SYMBOL:
                        case Character.CURRENCY_SYMBOL:
                        case Character.MODIFIER_SYMBOL:
                        case Character.OTHER_SYMBOL:
                            write(c);
                            break;
                        default:
                            write("\\u");
                            write(Integer.toHexString((c & 0xf000) >> 12));
                            write(Integer.toHexString((c & 0x0f00) >> 8));
                            write(Integer.toHexString((c & 0x00f0) >> 4));
                            write(Integer.toHexString(c & 0x000f));
                            break;
                    }
                    break;
            }
        }
        write('"');
    }

    private void write(final String str) {
        try {
            writer.write(str);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    private void write(final char c) {
        try {
            writer.write(c);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    private void checkPreconditions(final State... validStates) {
        if (!checkState(validStates)) {
            throw new BsonInvalidOperationException("Invalid state " + state);
        }
    }

    private boolean checkState(final State... validStates) {
        for (State cur : validStates) {
            if (cur == state) {
                return true;
            }
        }
        return false;

    }

    private void throwBSONException(final IOException e) {
        throw new BSONException("Wrapping IOException", e);
    }
}
