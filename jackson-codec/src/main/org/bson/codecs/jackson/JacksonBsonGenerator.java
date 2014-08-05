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

package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.Version;
import org.bson.BsonBinary;
import org.bson.BsonJavaScript;
import org.bson.BsonRegularExpression;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by guo on 7/18/14.
 */
public class JacksonBsonGenerator<T> extends JsonGenerator {

    private BsonWriter writer;
    private Class<T> clazz;

    public Class<T> getClazz() {
        return clazz;
    }

    //TODO figure out if this is necessary or if isClosed should always returne false
    private boolean isClosed = false;

    public JacksonBsonGenerator(final BsonWriter writer) {
        this.writer = writer;
    }

    public JacksonBsonGenerator(final BsonWriter writer, final Class<T> clazz) {
        this.clazz = clazz;
        this.writer = writer;
    }

    @Override
    public JsonGenerator setCodec(final ObjectCodec objectCodec) {
        //TODO: is this used?
        return null;
    }

    @Override
    public ObjectCodec getCodec() {
        //TODO: is this used?
        return null;
    }

    @Override
    public Version version() {
        //TODO
        return null;
    }

    @Override
    public JsonGenerator enable(final Feature feature) {
        //TODO
        return this;
    }

    @Override
    public JsonGenerator disable(final Feature feature) {
        //TODO
        return this;
    }

    @Override
    public boolean isEnabled(final Feature feature) {
        //: TODO
        return false;
    }

    @Override
    public int getFeatureMask() {
        //TODO
        return 0;
    }

    @Override
    public JsonGenerator setFeatureMask(final int i) {
        //TODO
        return null;
    }

    @Override
    public JsonGenerator useDefaultPrettyPrinter() {
        //TODO
        return null;
    }

    @Override
    public void writeStartArray() throws IOException {

        writer.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        writer.writeEndArray();
    }

    @Override
    public void writeStartObject() throws IOException {
        writer.writeStartDocument();
    }

    @Override
    public void writeEndObject() throws IOException {

        writer.writeEndDocument();
    }

    @Override
    public void writeFieldName(final String s) throws IOException {
        writer.writeName(s);
    }

    @Override
    public void writeFieldName(final SerializableString serializableString) throws IOException {

        writer.writeName(serializableString.getValue());
    }

    @Override
    public void writeString(final String s) throws IOException {
        writer.writeString(s);
    }

    @Override
    public void writeString(final char[] chars, final int i, final int i1) throws IOException {
        writer.writeString(new String(Arrays.copyOfRange(chars,i,i1)));
    }

    @Override
    public void writeString(final SerializableString serializableString) throws IOException {
        writer.writeString(serializableString.getValue());
    }

    @Override
    public void writeRawUTF8String(final byte[] bytes, final int i, final int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(Arrays.copyOfRange(bytes, i, i1)));
    }

    @Override
    public void writeUTF8String(final byte[] bytes, final int i, final int i1) throws IOException {
        writer.writeString(new String(bytes, "UTF8"));
    }

    @Override
    public void writeRaw(final String s) throws IOException {
        writer.writeBinaryData(new BsonBinary(s.getBytes()));
    }

    @Override
    public void writeRaw(final String s, final int i, final int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(s.substring(i,i1).getBytes()));
    }

    @Override
    public void writeRaw(final char[] chars, final int i, final int i1) throws IOException {
        writer.writeString(CharBuffer.wrap(chars).toString().substring(i, i1));
    }

    @Override
    public void writeRaw(final char c) throws IOException {
        writer.writeString(Character.toString(c));
    }

    @Override
    public void writeRawValue(final String s) throws IOException {
        writer.writeString(s );
    }

    @Override
    public void writeRawValue(final String s, final int i, final int i1) throws IOException {
        writer.writeString(s.substring(i,i1));
    }

    @Override
    public void writeRawValue(final char[] chars, final int i, final int i1) throws IOException {
        writer.writeString(CharBuffer.wrap(chars).toString().substring(i, i1));
    }

    @Override
    public void writeBinary(final Base64Variant base64Variant, final byte[] bytes, final int i, final int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(base64Variant.encode(bytes).getBytes()));
    }

    @Override
    public void writeBinary(final byte[] data) throws IOException {
        writer.writeBinaryData(new BsonBinary(data));
    }

    @Override
    public int writeBinary(final Base64Variant base64Variant, final InputStream inputStream, final int i) throws IOException {
        //TODO: inputStream
        return 0;
    }

    @Override
    public void writeNumber(final int i) throws IOException {
        writer.writeInt32(i);
    }

    @Override
    public void writeNumber(final long l) throws IOException {
        writer.writeInt64(l);
    }

    @Override
    public void writeNumber(final BigInteger bigInteger) throws IOException {
        writer.writeString(bigInteger.toString());
    }

    public void writeNumber(final double v) throws IOException {
        writer.writeDouble(v);
    }

    @Override
    public void writeNumber(final float v) throws IOException {
        writer.writeDouble((double)v);
    }

    @Override
    public void writeNumber(final BigDecimal bigDecimal) throws IOException {
        writer.writeString(bigDecimal.toString());
    }

    @Override
    public void writeNumber(final String s) throws IOException {
        writer.writeString(s);
    }

    @Override
    public void writeBoolean(final boolean b) throws IOException {
        writer.writeBoolean(b);
    }

    @Override
    public void writeNull() throws IOException {

        writer.writeNull();
    }

    @Override
    public void writeObject(final Object o) throws IOException {
        super._writeSimpleObject(o);
    }

    @Override
    public void writeTree(final TreeNode treeNode) throws IOException {
        //TODO: tree
    }

    public void writeDate(final Date date) throws IOException {
        writer.writeDateTime(date.getTime());
    }

    public void writeSymbol(final BsonSymbol symbol) throws IOException {
        writer.writeSymbol(symbol.getSymbol());
    }

    public void writeTimestamp(final BsonTimestamp timestamp) throws IOException{
       writer.writeTimestamp(timestamp);
    }

    public void writeJavascript(final BsonJavaScript javascript) throws IOException {
        writer.writeJavaScript(javascript.getCode());
    }

    public void writeRegex(final Pattern regex) throws IOException {
        writer.writeRegularExpression(new BsonRegularExpression(regex.pattern(), regex.flags()+""));
    }

    public void writeObjectId(final ObjectId id) throws IOException {
        writer.writeObjectId(id);
    }

    @Override
    public JsonStreamContext getOutputContext() {
        return null;
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }



}
