/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BsonUndefined;
import org.bson.internal.Base64;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SimpleTimeZone;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Defines static methods for getting {@code ObjectSerializer} instances that produce various flavors of JSON.
 *
 * @see org.bson.json.JsonReader
 * @see org.bson.json.JsonWriter
 * @see com.mongodb.BasicDBObject#toJson()
 * @see com.mongodb.BasicDBObject#parse(String)
 *
 * @deprecated This class has been superseded by to toJson and parse methods on BasicDBObject
 */
@Deprecated
@SuppressWarnings("deprecation")
public class JSONSerializers {

    private JSONSerializers() {
    }

    /**
     * Returns an {@code ObjectSerializer} that mostly conforms to the strict JSON format defined in
     * <a href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">extended JSON</a>, but with a few differences to keep
     * compatibility with previous versions of the driver.  Clients should generally prefer {@code getStrict} in preference to this method.
     *
     * @return object serializer
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
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
     * Returns an {@code ObjectSerializer} that conforms to the strict JSON format defined in
     * <a href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">extended JSON</a>.
     *
     * @return object serializer
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
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
        serializer.addObjectSerializer(DBRef.class, new DBRefBaseSerializer(serializer));
        serializer.addObjectSerializer(Iterable.class, new IterableSerializer(serializer));
        serializer.addObjectSerializer(Map.class, new MapSerializer(serializer));
        serializer.addObjectSerializer(MaxKey.class, new MaxKeySerializer(serializer));
        serializer.addObjectSerializer(MinKey.class, new MinKeySerializer(serializer));
        serializer.addObjectSerializer(Number.class, new ToStringSerializer());
        serializer.addObjectSerializer(ObjectId.class, new ObjectIdSerializer(serializer));
        serializer.addObjectSerializer(Pattern.class, new PatternSerializer(serializer));
        serializer.addObjectSerializer(String.class, new StringSerializer());
        serializer.addObjectSerializer(Symbol.class, new SymbolSerializer(serializer));
        serializer.addObjectSerializer(UUID.class, new UuidSerializer(serializer));
        serializer.addObjectSerializer(BsonUndefined.class, new UndefinedSerializer(serializer));
        serializer.addObjectSerializer(Decimal128.class, new Decimal128Serializer(serializer));
        return serializer;
    }

    private abstract static class CompoundObjectSerializer extends AbstractObjectSerializer {
        protected final ObjectSerializer serializer;

        CompoundObjectSerializer(final ObjectSerializer serializer) {
            this.serializer = serializer;
        }
    }

    private static class LegacyBinarySerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            buf.append("<Binary Data>");
        }

    }

    private static class ObjectArraySerializer extends CompoundObjectSerializer {

        ObjectArraySerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            buf.append("[ ");
            for (int i = 0; i < Array.getLength(obj); i++) {
                if (i > 0) {
                    buf.append(" , ");
                }
                serializer.serialize(Array.get(obj, i), buf);
            }

            buf.append("]");
        }

    }

    private static class ToStringSerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            buf.append(obj.toString());
        }

    }

    private static class LegacyBSONTimestampSerializer extends CompoundObjectSerializer {

        LegacyBSONTimestampSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            BSONTimestamp t = (BSONTimestamp) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$ts", Integer.valueOf(t.getTime()));
            temp.put("$inc", Integer.valueOf(t.getInc()));
            serializer.serialize(temp, buf);
        }

    }

    private static class CodeSerializer extends CompoundObjectSerializer {

        CodeSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            Code c = (Code) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$code", c.getCode());
            serializer.serialize(temp, buf);
        }

    }

    private static class CodeWScopeSerializer extends CompoundObjectSerializer {

        CodeWScopeSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            CodeWScope c = (CodeWScope) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$code", c.getCode());
            temp.put("$scope", c.getScope());
            serializer.serialize(temp, buf);
        }

    }

    private static class LegacyDateSerializer extends CompoundObjectSerializer {

        LegacyDateSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            Date d = (Date) obj;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
            serializer.serialize(new BasicDBObject("$date", format.format(d)), buf);
        }

    }

    private static class DBObjectSerializer extends CompoundObjectSerializer {

        DBObjectSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            boolean first = true;
            buf.append("{ ");
            DBObject dbo = (DBObject) obj;
            String name;

            for (final String s : dbo.keySet()) {
                name = s;

                if (first) {
                    first = false;
                } else {
                    buf.append(" , ");
                }

                JSON.string(buf, name);
                buf.append(" : ");
                serializer.serialize(dbo.get(name), buf);
            }

            buf.append("}");
        }

    }

    private static class DBRefBaseSerializer extends CompoundObjectSerializer {

        DBRefBaseSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            DBRef ref = (DBRef) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$ref", ref.getCollectionName());
            temp.put("$id", ref.getId());
            if (ref.getDatabaseName() != null) {
                temp.put("$db", ref.getDatabaseName());
            }
            serializer.serialize(temp, buf);
        }

    }

    private static class IterableSerializer extends CompoundObjectSerializer {

        IterableSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            boolean first = true;
            buf.append("[ ");

            for (final Object o : ((Iterable) obj)) {
                if (first) {
                    first = false;
                } else {
                    buf.append(" , ");
                }

                serializer.serialize(o, buf);
            }
            buf.append("]");
        }
    }

    private static class MapSerializer extends CompoundObjectSerializer {

        MapSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void serialize(final Object obj, final StringBuilder buf) {
            boolean first = true;
            buf.append("{ ");
            Map m = (Map) obj;
            Entry entry;

            for (final Object o : m.entrySet()) {
                entry = (Entry) o;
                if (first) {
                    first = false;
                } else {
                    buf.append(" , ");
                }
                JSON.string(buf, entry.getKey().toString());
                buf.append(" : ");
                serializer.serialize(entry.getValue(), buf);
            }

            buf.append("}");
        }

    }

    private static class MaxKeySerializer extends CompoundObjectSerializer {

        MaxKeySerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$maxKey", 1), buf);
        }

    }

    private static class MinKeySerializer extends CompoundObjectSerializer {

        MinKeySerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$minKey", 1), buf);
        }

    }

    private static class ObjectIdSerializer extends CompoundObjectSerializer {

        ObjectIdSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$oid", obj.toString()), buf);
        }
    }

    private static class PatternSerializer extends CompoundObjectSerializer {

        PatternSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            DBObject externalForm = new BasicDBObject();
            externalForm.put("$regex", obj.toString());
            if (((Pattern) obj).flags() != 0) {
                externalForm.put("$options", com.mongodb.Bytes.regexFlags(((Pattern) obj).flags()));
            }
            serializer.serialize(externalForm, buf);
        }
    }

    private static class StringSerializer extends AbstractObjectSerializer {

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            JSON.string(buf, (String) obj);
        }
    }

    private static class SymbolSerializer extends CompoundObjectSerializer {

        SymbolSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            Symbol symbol = (Symbol) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$symbol", symbol.toString());
            serializer.serialize(temp, buf);
        }
    }

    private static class UuidSerializer extends CompoundObjectSerializer {

        UuidSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            UUID uuid = (UUID) obj;
            BasicDBObject temp = new BasicDBObject();
            temp.put("$uuid", uuid.toString());
            serializer.serialize(temp, buf);
        }
    }

    private static class BSONTimestampSerializer extends CompoundObjectSerializer {

        BSONTimestampSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
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

        DateSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            Date d = (Date) obj;
            serializer.serialize(new BasicDBObject("$date", d.getTime()), buf);
        }

    }

    private abstract static class BinarySerializerBase extends CompoundObjectSerializer {
        BinarySerializerBase(final ObjectSerializer serializer) {
            super(serializer);
        }

        protected void serialize(final byte[] bytes, final byte type, final StringBuilder buf) {
            DBObject temp = new BasicDBObject();
            temp.put("$binary", Base64.encode(bytes));
            temp.put("$type", type);
            serializer.serialize(temp, buf);
        }
    }

    private static class BinarySerializer extends BinarySerializerBase {
        BinarySerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            Binary bin = (Binary) obj;
            serialize(bin.getData(), bin.getType(), buf);
        }

    }

    private static class ByteArraySerializer extends BinarySerializerBase {
        ByteArraySerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            serialize((byte[]) obj, (byte) 0, buf);
        }

    }

    private static class UndefinedSerializer extends CompoundObjectSerializer {

        UndefinedSerializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            BasicDBObject temp = new BasicDBObject();
            temp.put("$undefined", true);
            serializer.serialize(temp, buf);
        }

    }

    private static class Decimal128Serializer extends CompoundObjectSerializer {

        Decimal128Serializer(final ObjectSerializer serializer) {
            super(serializer);
        }

        @Override
        public void serialize(final Object obj, final StringBuilder buf) {
            serializer.serialize(new BasicDBObject("$numberDecimal", obj.toString()), buf);
        }
    }
}
