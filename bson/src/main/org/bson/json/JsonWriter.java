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

import org.bson.AbstractBsonWriter;
import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonContextType;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.lang.String.format;
import static javax.xml.bind.DatatypeConverter.printBase64Binary;

/**
 * A {@code BsonWriter} implementation that outputs a JSON representation of BSON.
 *
 * @since 3.0
 */
public class JsonWriter extends AbstractBsonWriter {
    private final Writer writer;
    private final JsonWriterSettings settings;

    /**
     * Creates a new instance which uses {@code writer} to write JSON to.
     *
     * @param writer the writer to write JSON to.
     */
    public JsonWriter(final Writer writer) {
        this(writer, new JsonWriterSettings());
    }

    /**
     * Creates a new instance which uses {@code writer} to write JSON to and uses the given settings.
     *
     * @param writer   the writer to write JSON to.
     * @param settings the settings to apply to this writer.
     */
    public JsonWriter(final Writer writer, final JsonWriterSettings settings) {
        super(settings);
        this.settings = settings;
        this.writer = writer;
        setContext(new Context(null, BsonContextType.TOP_LEVEL, ""));
    }

    /**
     * Gets the {@code Writer}.
     *
     * @return the writer
     */
    public Writer getWriter() {
        return writer;
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    @Override
    protected void doWriteStartDocument() {
        try {
            if (getState() == State.VALUE || getState() == State.SCOPE_DOCUMENT) {
                writeNameHelper(getName());
            }
            writer.write("{");

            BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
            setContext(new Context(getContext(), contextType, settings.getIndentCharacters()));
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteEndDocument() {
        try {
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
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteStartArray() {
        try {
            writeNameHelper(getName());
            writer.write("[");
            setContext(new Context(getContext(), BsonContextType.ARRAY, settings.getIndentCharacters()));
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteEndArray() {
        try {
            writer.write("]");
        } catch (IOException e) {
            throwBSONException(e);
        }
        setContext(getContext().getParentContext());
    }


    @Override
    protected void doWriteBinaryData(final BsonBinary binary) {
        try {
            switch (settings.getOutputMode()) {
                case SHELL:
                    writeNameHelper(getName());
                    writer.write(format("new BinData(%s, \"%s\")", Integer.toString(binary.getType() & 0xFF),
                            printBase64Binary(binary.getData())));

                    break;
                default:
                    writeStartDocument();
                    writeString("$binary", printBase64Binary(binary.getData()));
                    writeString("$type", Integer.toHexString(binary.getType() & 0xFF));
                    writeEndDocument();
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        try {
            writeNameHelper(getName());
            writer.write(value ? "true" : "false");
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteDateTime(final long value) {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeNameHelper("$date");
                    writer.write(Long.toString(value));
                    writeEndDocument();
                    break;
                case SHELL:
                    writeNameHelper(getName());

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    if (value >= -59014396800000L && value <= 253399536000000L) {
                        writer.write(format("ISODate(\"%s\")", dateFormat.format(new Date(value))));
                    } else {
                        writer.write(format("new Date(%d)", value));
                    }
                    break;
                default:
                    throw new BSONException("Unexpected JSONMode.");
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        writeStartDocument();
        writeString("$ref", value.getNamespace());
        writeObjectId("$id", value.getId());
        writeEndDocument();
    }

    @Override
    protected void doWriteDouble(final double value) {
        try {
            writeNameHelper(getName());
            writer.write(Double.toString(value));
            setState(getNextState());
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteInt32(final int value) {
        try {
            writeNameHelper(getName());
            writer.write(Integer.toString(value));
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteInt64(final long value) {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeNameHelper("$numberLong");
                    writer.write(format("\"%d\"", value));
                    writeEndDocument();
                    break;
                case SHELL:
                    writeNameHelper(getName());
                    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                        writer.write(format("NumberLong(%d)", value));
                    } else {
                        writer.write(format("NumberLong(\"%d\")", value));
                    }
                    break;
                default:
                    writeNameHelper(getName());
                    writer.write(Long.toString(value));
                    break;
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteDecimal128(final Decimal128 value) {
        try {
            switch (settings.getOutputMode()) {
                case SHELL:
                    writeNameHelper(getName());
                    writer.write(format("NumberDecimal(\"%s\")", value.toString()));
                    break;
                case STRICT:
                default:
                    writeStartDocument();
                    writeNameHelper("$numberDecimal");
                    writer.write(format("\"%s\"", value.toString()));
                    writeEndDocument();
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    protected void doWriteJavaScript(final String code) {
        writeStartDocument();
        writeString("$code", code);
        writeEndDocument();
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String code) {
        writeStartDocument();
        writeString("$code", code);
        writeName("$scope");
    }

    @Override
    protected void doWriteMaxKey() {
        writeStartDocument();
        writeInt32("$maxKey", 1);
        writeEndDocument();
    }

    @Override
    protected void doWriteMinKey() {
        writeStartDocument();
        writeInt32("$minKey", 1);
        writeEndDocument();
    }

    @Override
    public void doWriteNull() {
        try {
            writeNameHelper(getName());
            writer.write("null");
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteObjectId(final ObjectId objectId) {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeString("$oid", objectId.toString());
                    writeEndDocument();
                    break;
                case SHELL:
                    writeNameHelper(getName());
                    writer.write(format("ObjectId(\"%s\")", objectId.toString()));
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression regularExpression) {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeString("$regex", regularExpression.getPattern());
                    writeString("$options", regularExpression.getOptions());
                    writeEndDocument();
                    break;
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
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteString(final String value) {
        try {
            writeNameHelper(getName());
            writeStringHelper(value);
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteSymbol(final String value) {
        writeStartDocument();
        writeString("$symbol", value);
        writeEndDocument();
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeStartDocument("$timestamp");
                    writeInt32("t", value.getTime());
                    writeInt32("i", value.getInc());
                    writeEndDocument();
                    writeEndDocument();
                    break;
                case SHELL:
                    writeNameHelper(getName());
                    writer.write(format("Timestamp(%d, %d)", value.getTime(), value.getInc()));
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void doWriteUndefined() {
        try {
            switch (settings.getOutputMode()) {
                case STRICT:
                    writeStartDocument();
                    writeBoolean("$undefined", true);
                    writeEndDocument();
                    break;
                case SHELL:
                    writeNameHelper(getName());
                    writer.write("undefined");
                    break;
                default:
                    throw new BSONException("Unknown output mode" + settings.getOutputMode());
            }
        } catch (IOException e) {
            throwBSONException(e);
        }
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            throwBSONException(e);
        }
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

/**
 * The context for the writer, inheriting all the values from {@link org.bson.AbstractBsonWriter.Context}, and additionally providing
 * settings for the indentation level and whether there are any child elements at this level.
 */
public class Context extends AbstractBsonWriter.Context {
    private final String indentation;
    private boolean hasElements;

    /**
     * Creates a new context.
     *
     * @param parentContext the parent context that can be used for going back up to the parent level
     * @param contextType   the type of this context
     * @param indentChars   the String to use for indentation at this level.
     */
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
