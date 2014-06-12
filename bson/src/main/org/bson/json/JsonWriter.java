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

import org.bson.AbstractBsonWriter;
import org.bson.BSONException;
import org.bson.BsonContextType;
import org.bson.BsonTimestamp;
import org.bson.types.Binary;
import org.bson.types.BsonDbPointer;
import org.bson.types.BsonRegularExpression;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static javax.xml.bind.DatatypeConverter.printBase64Binary;

/**
 * A {@code BsonWriter} implementation that outputs a JSON representation of BSON.
 *
 * @since 3.0
 */
public class JsonWriter extends AbstractBsonWriter {
    private final Writer writer;
    private final JsonWriterSettings settings;

    public JsonWriter(final Writer writer) {
        this(writer, new JsonWriterSettings());
    }

    public JsonWriter(final Writer writer, final JsonWriterSettings settings) {
        super(settings);
        this.settings = settings;
        this.writer = writer;
        setContext(new Context(null, BsonContextType.TOP_LEVEL, ""));
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
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
        checkPreconditions("writeStartDocument", State.INITIAL, State.VALUE, State.SCOPE_DOCUMENT);

        try {
            super.writeStartDocument();
            if (getState() == State.VALUE || getState() == State.SCOPE_DOCUMENT) {
                writeNameHelper(getName());
            }
            writer.write("{");

            BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
            setContext(new Context(getContext(), contextType, settings.getIndentCharacters()));
            setState(State.NAME);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeEndDocument() {
        checkPreconditions("writeEndDocument", State.NAME);

        try {
            super.writeEndDocument();
            if (settings.isIndent() && getContext().hasElements) {
                writer.write(settings.getNewLineCharacters());
                if (getContext().getParentContext() != null) {
                    writer.write(getContext().getParentContext().indentation);
                }
                writer.write("}");
            } else {
                writer.write(" }");
            }

            if (getContext().getContextType() == BsonContextType.SCOPE_DOCUMENT) {
                setContext(getContext().getParentContext());
                writeEndDocument();
            } else {
                setContext(getContext().getParentContext());
            }

            if (getContext() == null) {
                setState(State.DONE);
            } else {
                setState(getNextState());
            }
        } catch (IOException e) {
            throwBSONException(e);
        }

    }

    @Override
    public void writeStartArray() {
        checkPreconditions("writeStartArray", State.VALUE, State.INITIAL);

        try {
            super.writeStartArray();
            writeNameHelper(getName());
            writer.write("[");

            setContext(new Context(getContext(), BsonContextType.ARRAY, settings.getIndentCharacters()));
            setState(State.VALUE);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeEndArray() {
        checkPreconditions("writeEndArray", State.VALUE);

        try {
            super.writeEndArray();
            writer.write("]");

            setContext(getContext().getParentContext());
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        checkPreconditions("writeBinaryData", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case SHELL:
                    writeNameHelper(getName());
                    writer.write(String.format("new BinData(%s, \"%s\")", Integer.toString(binary.getType() & 0xFF),
                                               printBase64Binary(binary.getData())));

                    break;
                default:
                    writeStartDocument();
                    writeString("$binary", printBase64Binary(binary.getData()));
                    writeString("$type", Integer.toHexString(binary.getType() & 0xFF));
                    writeEndDocument();
            }
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeBoolean(final boolean value) {
        checkPreconditions("writeBoolean", State.VALUE, State.INITIAL);

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
        checkPreconditions("writeDateTime", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeInt64("$date", value);
                    writeEndDocument();
                    break;
                case JAVASCRIPT:
                case TEN_GEN:
                    writeNameHelper(getName());
                    writer.write(String.format("new Date(%d)", value));
                    break;
                case SHELL:
                    writeNameHelper(getName());

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    if (value >= -59014396800000L && value <= 253399536000000L) {
                        writer.write(String.format("ISODate(\"%s\")", dateFormat.format(new Date(value))));
                    } else {
                        writer.write(String.format("new Date(%d)", value));
                    }
                    break;
                default:
                    throw new BSONException("Unexpected JSONMode.");
            }

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeDouble(final double value) {
        checkPreconditions("writeDouble", State.VALUE, State.INITIAL);
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
        checkPreconditions("writeInt32", State.VALUE, State.INITIAL);

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
        checkPreconditions("writeInt64", State.VALUE, State.INITIAL);

        try {
            writeNameHelper(getName());
            switch (settings.getOutputMode()) {
                case STRICT:
                case JAVASCRIPT:
                    writer.write(Long.toString(value));
                    break;
                case TEN_GEN:
                case SHELL:
                    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                        writer.write(String.format("NumberLong(%d)", value));
                    } else {
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
        checkPreconditions("writeJavaScript", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeString("$code", code);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        checkPreconditions("writeJavaScriptWithScope", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeString("$code", code);
        writeName("$scope");

        setState(State.SCOPE_DOCUMENT);
    }

    @Override
    public void writeMaxKey() {
        checkPreconditions("writeMaxKey", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeInt32("$maxKey", 1);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeMinKey() {
        checkPreconditions("writeMinKey", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeInt32("$minKey", 1);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeNull() {
        checkPreconditions("writeNull", State.VALUE, State.INITIAL);

        try {
            writeNameHelper(getName());
            writer.write("null");

            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        checkPreconditions("writeObjectId", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                case JAVASCRIPT:
                    writeStartDocument();
                    writeString("$oid", objectId.toString());
                    writeEndDocument();
                    break;
                case TEN_GEN:
                case SHELL:
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
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        checkPreconditions("writeRegularExpression", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeString("$regex", regularExpression.getPattern());
                    writeString("$options", regularExpression.getOptions());
                    writeEndDocument();
                    break;
                case JAVASCRIPT:
                case TEN_GEN:
                case SHELL:
                    writeNameHelper(getName());
                    writer.write("/");
                    String escaped = (regularExpression.getPattern().equals("")) ? "(?:)" : regularExpression.getPattern()
                                                                                                             .replace("/", "\\/");
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
        checkPreconditions("writeString", State.VALUE, State.INITIAL);

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
        checkPreconditions("writeSymbol", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeString("$symbol", value);
        writeEndDocument();

        setState(getNextState());
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        checkPreconditions("writeTimestamp", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                case JAVASCRIPT:
                    writeStartDocument();
                    writeStartDocument("$timestamp");
                    writeInt32("t", value.getTime());
                    writeInt32("i", value.getInc());
                    writeEndDocument();
                    writeEndDocument();
                    break;
                case TEN_GEN:
                case SHELL:
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
        checkPreconditions("writeUndefined", State.VALUE, State.INITIAL);

        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeBoolean("$undefined", true);
                    writeEndDocument();
                    break;
                case JAVASCRIPT:
                case TEN_GEN:
                case SHELL:
                    writeNameHelper(getName());
                    writer.write("undefined");
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
    public void writeDBPointer(final BsonDbPointer value) {
        checkPreconditions("writeDBPointer", State.VALUE, State.INITIAL);

        writeStartDocument();
        writeString("$ref", value.getNamespace());
        writeObjectId("$id", value.getId());
        writeEndDocument();


        setState(getNextState());
    }

    private void writeNameHelper(final String name) throws IOException {
        switch (getContext().getContextType()) {
            case ARRAY:
                // don't write Array element names in JSON
                if (getContext().hasElements) {
                    writer.write(", ");
                }
                break;
            case DOCUMENT:
            case SCOPE_DOCUMENT:
                if (getContext().hasElements) {
                    writer.write(",");
                }
                if (settings.isIndent()) {
                    writer.write(settings.getNewLineCharacters());
                    writer.write(getContext().indentation);
                } else {
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

        getContext().hasElements = true;
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

    public class Context extends AbstractBsonWriter.Context {
        private final String indentation;
        private boolean hasElements;

        public Context(final Context parentContext, final BsonContextType contextType, final String indentChars) {
            super(parentContext, contextType);
            this.indentation = (parentContext == null) ? indentChars : parentContext.indentation + indentChars;
        }

        @Override
        public Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}
