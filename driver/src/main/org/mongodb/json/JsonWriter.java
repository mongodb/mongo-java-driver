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

import org.bson.BSONException;
import org.bson.BSONWriter;
import org.bson.ContextType;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.util.Base64Codec;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A {@code BSONWriter} implementation that outputs a JSON representation of BSON.
 *
 * @since 3.0.0
 */
public class JsonWriter extends BSONWriter {
    private final Writer writer;
    private Context context;
    private final JsonWriterSettings settings;

    public JsonWriter(final Writer writer) {
        this(writer, new JsonWriterSettings());
    }

    public JsonWriter(final Writer writer, final JsonWriterSettings settings) {
        super(settings);
        this.settings = settings;
        this.writer = writer;
        context = new Context(null, ContextType.TOP_LEVEL, "");
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeStartDocument() {
        try {
            super.writeStartDocument();
            if (getState() == State.VALUE || getState() == State.SCOPE_DOCUMENT) {
                writeNameHelper(getName());
            }
            writer.write("{");

            final ContextType contextType = (getState()
                    == State.SCOPE_DOCUMENT) ? ContextType.SCOPE_DOCUMENT : ContextType.DOCUMENT;
            context = new Context(context, contextType, settings.getIndentCharacters());
            setState(State.NAME);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeEndDocument() {
        try {
            super.writeEndDocument();
            if (settings.isIndent() && context.hasElements) {
                writer.write(settings.getNewLineCharacters());
                if (context.parentContext != null) {
                    writer.write(context.parentContext.indentation);
                }
                writer.write("}");
            }
            else {
                writer.write(" }");
            }

            if (context.contextType == ContextType.SCOPE_DOCUMENT) {
                context = context.parentContext;
                writeEndDocument();
            }
            else {
                context = context.parentContext;
            }

            if (context == null) {
                setState(State.DONE);
            }
            else {
                setState(getNextState());
            }
        } catch (IOException e) {
            throwBSONException(e);
        }

    }

    @Override
    public void writeStartArray() {
        try {
            super.writeStartArray();
            writeNameHelper(getName());
            writer.write("[");

            context = new Context(context, ContextType.ARRAY, settings.getIndentCharacters());
            setState(State.VALUE);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeEndArray() {
        try {
            super.writeEndArray();
            writer.write("]");

            context = context.parentContext;
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        try {
            switch (settings.getOutputMode()) {
                case Shell:
                    writeNameHelper(getName());
                    writer.write(String.format("new BinData(%s, \"%s\")", Integer.toHexString(binary.getType()),
                                               new Base64Codec().encode(binary.getData())));
                    break;
                default:
                    writeStartDocument();
                    writeString("$binary", new Base64Codec().encode(binary.getData()));
                    writeString("$type", Integer.toHexString(binary.getType()));
                    writeEndDocument();
            }
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeBoolean(final boolean value) {
        try {
            writeNameHelper(getName());
            writer.write(value ? "true" : "false");
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeDateTime(final long value) {
        try {
            switch (settings.getOutputMode()) {
                case Strict:
                    writeStartDocument();
                    writeInt64("$date", value);
                    writeEndDocument();
                    break;
                case JavaScript:
                case TenGen:
                    writeNameHelper(getName());
                    writer.write(String.format("new Date(%d)", value));
                    break;
                case Shell:
                    writeNameHelper(getName());

                    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    if (value >= -59014396800000L && value <= 253399536000000L) {
                        writer.write(String.format("ISODate(\"%s\")", dateFormat.format(new Date(value))));
                    }
                    else {
                        writer.write(String.format("new Date(%d)", value));
                    }
                    break;
                default:
                    throw new BSONException("Unexpected JsonOutputMode.");
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeDouble(final double value) {
        try {
            writeNameHelper(getName());
            writer.write(Double.toString(value));
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeInt32(final int value) {
        try {
            writeNameHelper(getName());
            writer.write(Integer.toString(value));
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeInt64(final long value) {
        try {
            writeNameHelper(getName());
            switch (settings.getOutputMode()) {
                case Strict:
                case JavaScript:
                    writer.write(Long.toString(value));
                    break;
                case TenGen:
                case Shell:
                    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                        writer.write(String.format("NumberLong(%d)", value));
                    }
                    else {
                        writer.write(String.format("NumberLong(\"%d\")", value));
                    }
                    break;
                default:
                    writer.write(Long.toString(value));
                    break;
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeJavaScript(final String code) {

        writeStartDocument();
        writeString("$code", code);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        writeStartDocument();
        writeString("$code", code);
        writeName("$scope");

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        writeStartDocument();
        writeInt32("$maxkey", 1);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        writeStartDocument();
        writeInt32("$minkey", 1);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeNull() {
        try {
            writeNameHelper(getName());
            writer.write("null");
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        try {
            switch (settings.getOutputMode()) {
                case Strict:
                case JavaScript:
                    writeStartDocument();
                    writeString("$oid", objectId.toString());
                    writeEndDocument();
                    break;
                case TenGen:
                case Shell:
                    writeNameHelper(getName());
                    writer.write(String.format("ObjectId(\"%s\")", objectId.toString()));
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeRegularExpression(final RegularExpression regularExpression) {
        try {
            switch (settings.getOutputMode()) {
                case Strict:
                    writeStartDocument();
                    writeString("$regex", regularExpression.getPattern());
                    writeString("$options", regularExpression.getOptions());
                    writeEndDocument();
                    break;
                case JavaScript:
                case TenGen:
                case Shell:
                    writeNameHelper(getName());
                    writer.write("/");
                    final String escaped = (regularExpression.getPattern().equals("")) ? "(?:)" : regularExpression
                            .getPattern().replace("/", "\\/");
                    writer.write(escaped);
                    writer.write("/");
                    writer.write(regularExpression.getOptions());
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeString(final String value) {
        try {
            writeNameHelper(getName());
            writeStringHelper(value);
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeSymbol(final String value) {
        writeStartDocument();
        writeString("$symbol", value);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final BSONTimestamp value) {
        try {
            switch (settings.getOutputMode()) {
                case Strict:
                case JavaScript:
                    writeStartDocument();
                    writeStartDocument("$timestamp");
                    writeInt32("t", value.getTime());
                    writeInt32("i", value.getInc());
                    writeEndDocument();
                    writeEndDocument();
                    break;
                case TenGen:
                case Shell:
                    writeNameHelper(getName());
                    writer.write(String.format("Timestamp(%d, %d)", value.getTime(), value.getInc()));
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }

        setState(getNextState());
    }

    @Override
    public void writeUndefined() {
        try {
            writeNameHelper(getName());
            writer.write("undefined");

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    private State getNextState() {
        if (context.contextType == ContextType.ARRAY) {
            return State.VALUE;
        }
        else {
            return State.NAME;
        }
    }

    private void writeNameHelper(final String name) throws IOException {
        switch (context.contextType) {
            case ARRAY:
                // don't write Array element names in Json
                if (context.hasElements) {
                    writer.write(", ");
                }
                break;
            case DOCUMENT:
            case SCOPE_DOCUMENT:
                if (context.hasElements) {
                    writer.write(",");
                }
                if (settings.isIndent()) {
                    writer.write(settings.getNewLineCharacters());
                    writer.write(context.indentation);
                }
                else {
                    writer.write(" ");
                }
                writeStringHelper(name);
                writer.write(" : ");
                break;
            case TOP_LEVEL:
                break;
            default:
                throw new BSONException("Invalid contextType.");
        }

        context.hasElements = true;
    }

    private void writeStringHelper(final String str) throws IOException {
        writer.write('"');
        for (final char c : str.toCharArray()) {
            switch (c) {
                case '"':
                    writer.write("\\\"");
                    break;
                case '\\':
                    writer.write("\\\\");
                    break;
                case '\b':
                    writer.write("\\b");
                    break;
                case '\f':
                    writer.write("\\f");
                    break;
                case '\n':
                    writer.write("\\n");
                    break;
                case '\r':
                    writer.write("\\r");
                    break;
                case '\t':
                    writer.write("\\t");
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
                            writer.write(c);
                            break;
                        default:
                            writer.write("\\u");
                            writer.write(Integer.toHexString((c & 0xf000) >> 12));
                            writer.write(Integer.toHexString((c & 0x0f00) >> 8));
                            writer.write(Integer.toHexString((c & 0x00f0) >> 4));
                            writer.write(Integer.toHexString(c & 0x000f));
                            break;
                    }
                    break;
            }
        }
        writer.write('"');
    }

    private void throwBSONException(final IOException e) {
        throw new BSONException("Wrapping IOException", e);
    }

    class Context {
        private final Context parentContext;
        private final ContextType contextType;
        private final String indentation;
        private boolean hasElements;

        public Context(final Context parentContext, final ContextType contextType, final String indentChars) {
            this.parentContext = parentContext;
            this.contextType = contextType;
            this.indentation = (parentContext == null) ? indentChars : parentContext.indentation + indentChars;
        }
    }
}
