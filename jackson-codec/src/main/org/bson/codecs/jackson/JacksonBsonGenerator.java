package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.*;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.security.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by guo on 7/18/14.
 */
public class JacksonBsonGenerator<T> extends JsonGenerator {

    private BsonWriter writer;

    //TODO figure out if this is necessary or if isClosed should always returne false
    private boolean isClosed = false;

    public JacksonBsonGenerator(BsonWriter writer) {
        this.writer = writer;
    }

    @Override
    public JsonGenerator setCodec(ObjectCodec objectCodec) {
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
    public JsonGenerator enable(Feature feature) {
        //TODO
        return this;
    }

    @Override
    public JsonGenerator disable(Feature feature) {
        //TODO
        return this;
    }

    @Override
    public boolean isEnabled(Feature feature) {
        //: TODO
        return false;
    }

    @Override
    public int getFeatureMask() {
        //TODO
        return 0;
    }

    @Override
    public JsonGenerator setFeatureMask(int i) {
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
    public void writeFieldName(String s) throws IOException {
        writer.writeName(s);
    }

    @Override
    public void writeFieldName(SerializableString serializableString) throws IOException {

        writer.writeName(serializableString.getValue());
    }

    @Override
    public void writeString(String s) throws IOException {
        writer.writeString(s);
    }

    @Override
    public void writeString(char[] chars, int i, int i1) throws IOException {
        writer.writeString(new String(Arrays.copyOfRange(chars,i,i1)));
    }

    @Override
    public void writeString(SerializableString serializableString) throws IOException {
        writer.writeString(serializableString.getValue());
    }

    @Override
    public void writeRawUTF8String(byte[] bytes, int i, int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(Arrays.copyOfRange(bytes, i, i1)));
    }

    @Override
    public void writeUTF8String(byte[] bytes, int i, int i1) throws IOException {
        writer.writeString(new String(bytes, "UTF8"));
    }

    @Override
    public void writeRaw(String s) throws IOException {
        writer.writeBinaryData(new BsonBinary(s.getBytes()));
    }

    @Override
    public void writeRaw(String s, int i, int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(s.substring(i,i1).getBytes()));
    }

    @Override
    public void writeRaw(char[] chars, int i, int i1) throws IOException {
        writer.writeString(CharBuffer.wrap(chars).toString().substring(i, i1));
    }

    @Override
    public void writeRaw(char c) throws IOException {
        writer.writeString(Character.toString(c));
    }

    @Override
    public void writeRawValue(String s) throws IOException {
        writer.writeString(s );
    }

    @Override
    public void writeRawValue(String s, int i, int i1) throws IOException {
        writer.writeString(s.substring(i,i1));
    }

    @Override
    public void writeRawValue(char[] chars, int i, int i1) throws IOException {
        writer.writeString(CharBuffer.wrap(chars).toString().substring(i, i1));
    }

    @Override
    public void writeBinary(Base64Variant base64Variant, byte[] bytes, int i, int i1) throws IOException {
        writer.writeBinaryData(new BsonBinary(base64Variant.encode(bytes).getBytes()));
    }

    @Override
    public void writeBinary(byte[] data) throws IOException {
        writer.writeBinaryData(new BsonBinary(data));
    }

    @Override
    public int writeBinary(Base64Variant base64Variant, InputStream inputStream, int i) throws IOException {
        //TODO: inputStream
        return 0;
    }

    @Override
    public void writeNumber(int i) throws IOException {
        writer.writeInt32(i);
    }

    @Override
    public void writeNumber(long l) throws IOException {
        writer.writeInt64(l);
    }

    @Override
    public void writeNumber(BigInteger bigInteger) throws IOException {
        writer.writeString(bigInteger.toString());
    }

    public void writeNumber(double v) throws IOException {
        writer.writeDouble(v);
    }

    @Override
    public void writeNumber(float v) throws IOException {
        writer.writeDouble((double)v);
    }

    @Override
    public void writeNumber(BigDecimal bigDecimal) throws IOException {
        writer.writeString(bigDecimal.toString());
    }

    @Override
    public void writeNumber(String s) throws IOException {
        writer.writeString(s);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        writer.writeBoolean(b);
    }

    @Override
    public void writeNull() throws IOException {

        writer.writeNull();
    }

    @Override
    public void writeObject(Object o) throws IOException {
        super._writeSimpleObject(o);
    }

    @Override
    public void writeTree(TreeNode treeNode) throws IOException {
        //TODO: tree
    }

    public void writeDate(Date date) throws IOException {
        writer.writeDateTime(date.getTime());
    }

    public void writeSymbol(BsonSymbol symbol) throws IOException {
        writer.writeSymbol(symbol.getSymbol());
    }

    public void writeTimestamp(BsonTimestamp timestamp) throws IOException{
       writer.writeTimestamp(timestamp);
    }

    public void writeJavascript(BsonJavaScript javascript) throws IOException {
        writer.writeJavaScript(javascript.getCode());
    }

    public void writeRegex(Pattern regex) throws IOException {
        writer.writeRegularExpression(new BsonRegularExpression(regex.pattern(), regex.flags()+""));
    }

    public void writeObjectId(ObjectId id) throws IOException {
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
