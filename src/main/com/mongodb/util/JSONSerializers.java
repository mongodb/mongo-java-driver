/**
 *      Copyright (C) 2012 10gen Inc.
 *  
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBObject;
import com.mongodb.DBRefBase;
import org.bson.types.*;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * Defines static methods for getting <code>ObjectSerializer</code> instances that produce various flavors of
 * JSON.
 */
public class JSONSerializers {

    private JSONSerializers() {
    }

    /**
     * Returns an <code>ObjectSerializer</code> that mostly conforms to the strict JSON format defined in
     * <a href="http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON", but with a few differences to keep
     * compatibility with previous versions of the driver.  Clients should generally prefer
     * <code>getStrict</code> in preference to this method.
     *
     * @return object serializer
     * @see #getStrict()
     */
    public static ObjectSerializer getLegacy() {

        ClassMapBasedObjectSerializer serializer = addCommonSerializers();

        serializer.addObjectSerializer(Date.class, new LegacyDateSerializer(serializer));
        serializer.addObjectSerializer(BSONTimestamp.class, new LegacyBSONTimestampSerializer(serializer));
        serializer.addObjectSerializer(Binary.class, new LegacyBinarySerializer());
        serializer.addObjectSerializer(byte[].class, new LegacyBinarySerializer());
        return serializer;
    }

    /**
     * Returns an <code>ObjectSerializer</code> that conforms to the strict JSON format defined in
     * <a href="http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON".
     *
     * @return object serializer
     */
    public static ObjectSerializer getStrict() {

        ClassMapBasedObjectSerializer serializer = addCommonSerializers();

        serializer.addObjectSerializer(Date.class, new DateSerializer(serializer));
        serializer.addObjectSerializer(BSONTimestamp.class, new BSONTimestampSerializer(serializer));
        serializer.addObjectSerializer(Binary.class, new BinarySerializer(serializer));
        serializer.addObjectSerializer(byte[].class, new ByteArraySerializer(serializer));

        return serializer;
    }

    static ClassMapBasedObjectSerializer addCommonSerializers() {
        ClassMapBasedObjectSerializer serializer = new ClassMapBasedObjectSerializer();

        serializer.addObjectSerializer(Object[].class, new ObjectArraySerializer(serializer));
        serializer.addObjectSerializer(Boolean.class, new ToStringSerializer());
        serializer.addObjectSerializer(Code.class, new CodeSerializer(serializer));
        serializer.addObjectSerializer(CodeWScope.class, new CodeWScopeSerializer(serializer));
        serializer.addObjectSerializer(DBObject.class, new DBObjectSerializer(serializer));
        serializer.addObjectSerializer(DBRefBase.class, new DBRefBaseSerializer(serializer));
        serializer.addObjectSerializer(Iterable.class, new IterableSerializer(serializer));
        serializer.addObjectSerializer(Map.class, new MapSerializer(serializer));
        serializer.addObjectSerializer(MaxKey.class, new MaxKeySerializer(serializer));
        serializer.addObjectSerializer(MinKey.class, new MinKeySerializer(serializer));
        serializer.addObjectSerializer(Number.class, new ToStringSerializer());
        serializer.addObjectSerializer(ObjectId.class, new ObjectIdSerializer(serializer));
        serializer.addObjectSerializer(Pattern.class, new PatternSerializer(serializer));
        serializer.addObjectSerializer(String.class, new StringSerializer());
        serializer.addObjectSerializer(UUID.class, new UUIDSerializer(serializer));
        return serializer;
    }

    private abstract static class CompoundObjectSerializer extends AbstractObjectSerializer {
        protected final ObjectSerializer serializer;

        CompoundObjectSerializer(ObjectSerializer serializer) {
            this.serializer = serializer;
        }
    }

    private static class LegacyBinarySerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            buf.append("<Binary Data>");
        }

    }

    private static class ObjectArraySerializer extends CompoundObjectSerializer {

        ObjectArraySerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            buf.append("[ ");
            for (int i = 0; i < Array.getLength(obj); i++) {
                if (i > 0)
                    buf.append(" , ");
                serializer.serialize(Array.get(obj, i), buf);
            }

            buf.append("]");
        }

    }

    private static class ToStringSerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            buf.append(obj.toString());
        }

    }

    private static class LegacyBSONTimestampSerializer extends CompoundObjectSerializer {

        LegacyBSONTimestampSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            BSONTimestamp t = (BSONTimestamp) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$ts", Integer.valueOf(t.getTime()));
            temp.put("$inc", Integer.valueOf(t.getInc()));
            serializer.serialize(temp, buf);
        }

    }

    private static class CodeSerializer extends CompoundObjectSerializer {

        CodeSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            Code c = (Code) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$code", c.getCode());
            serializer.serialize(temp, buf);
        }

    }

    private static class CodeWScopeSerializer extends CompoundObjectSerializer {

        CodeWScopeSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            CodeWScope c = (CodeWScope) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$code", c.getCode());
            temp.put("$scope", c.getScope());
            serializer.serialize(temp, buf);
        }

    }

    private static class LegacyDateSerializer extends CompoundObjectSerializer {

        LegacyDateSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            Date d = (Date) obj;
            SimpleDateFormat format = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            format.setCalendar(new GregorianCalendar(
                    new SimpleTimeZone(0, "GMT")));
            serializer.serialize(
                    new BasicDBObject("$date", format.format(d)),
                    buf);
        }

    }

    private static class DBObjectSerializer extends CompoundObjectSerializer {

        DBObjectSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            boolean first = true;
            buf.append("{ ");
            DBObject dbo = (DBObject) obj;
            String name;

            for (final String s : dbo.keySet()) {
                name = s;

                if (first)
                    first = false;
                else
                    buf.append(" , ");

                JSON.string(buf, name);
                buf.append(" : ");
                serializer.serialize(dbo.get(name), buf);
            }

            buf.append("}");
        }

    }

    private static class DBRefBaseSerializer extends CompoundObjectSerializer {

        DBRefBaseSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            DBRefBase ref = (DBRefBase) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$ref", ref.getRef());
            temp.put("$id", ref.getId());
            serializer.serialize(temp, buf);
        }

    }

    private static class IterableSerializer extends CompoundObjectSerializer {

        IterableSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            boolean first = true;
            buf.append("[ ");

            for (final Object o : ((Iterable) obj)) {
                if (first)
                    first = false;
                else
                    buf.append(" , ");

                serializer.serialize(o, buf);
            }
            buf.append("]");
        }
    }

    private static class MapSerializer extends CompoundObjectSerializer {

        MapSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            boolean first = true;
            buf.append("{ ");
            Map m = (Map) obj;
            Entry entry;

            for (final Object o : m.entrySet()) {
                entry = (Entry) o;
                if (first)
                    first = false;
                else
                    buf.append(" , ");
                JSON.string(buf, entry.getKey().toString());
                buf.append(" : ");
                serializer.serialize(entry.getValue(), buf);
            }

            buf.append("}");
        }

    }

    private static class MaxKeySerializer extends CompoundObjectSerializer {

        MaxKeySerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$maxKey",
                    1), buf);
        }

    }

    private static class MinKeySerializer extends CompoundObjectSerializer {

        MinKeySerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$minKey",
                    1), buf);
        }

    }

    private static class ObjectIdSerializer extends CompoundObjectSerializer {

        ObjectIdSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            serializer.serialize(
                    new BasicDBObject("$oid", obj.toString()), buf);
        }
    }

    private static class PatternSerializer extends CompoundObjectSerializer {

        PatternSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            DBObject externalForm = new BasicDBObject();
            externalForm.put("$regex", obj.toString());
            if (((Pattern) obj).flags() != 0)
                externalForm.put("$options",
                        Bytes.regexFlags(((Pattern) obj).flags()));
            serializer.serialize(externalForm, buf);
        }
    }

    private static class StringSerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            JSON.string(buf, (String) obj);
        }
    }

    private static class UUIDSerializer extends CompoundObjectSerializer {

        UUIDSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            UUID uuid = (UUID) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$uuid", uuid.toString());
            serializer.serialize(temp, buf);
        }
    }

    private static class BSONTimestampSerializer extends CompoundObjectSerializer {

        BSONTimestampSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            BSONTimestamp t = (BSONTimestamp) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("t", Integer.valueOf(t.getTime()));
            temp.put("i", Integer.valueOf(t.getInc()));
            BasicDBObject timestampObj = new BasicDBObject();
            timestampObj.put("$timestamp", temp);
            serializer.serialize(timestampObj, buf);
        }

    }

    private static class DateSerializer extends CompoundObjectSerializer {

        DateSerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj,  StringBuilder buf) {
            Date d = (Date) obj;
            serializer.serialize(
                    new BasicDBObject("$date", d.getTime()), buf);
        }

    }

    private abstract static class BinarySerializerBase extends CompoundObjectSerializer {
        BinarySerializerBase(ObjectSerializer serializer) {
            super(serializer);
        }

        protected void serialize(byte[] bytes, byte type, StringBuilder buf) {
            DBObject temp = new BasicDBObject();
            temp.put("$binary",
                    (new Base64Codec()).encode(bytes));
            temp.put("$type", type);
            serializer.serialize(temp, buf);
        }
    }

    private static class BinarySerializer extends BinarySerializerBase {
        BinarySerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            Binary bin = (Binary) obj;
            serialize(bin.getData(), bin.getType(), buf);
        }

    }

    private static class ByteArraySerializer extends BinarySerializerBase {
        ByteArraySerializer(ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(Object obj, StringBuilder buf) {
            serialize((byte[]) obj, (byte) 0,  buf);
        }

    }
}
