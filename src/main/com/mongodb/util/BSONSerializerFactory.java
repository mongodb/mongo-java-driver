
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

import com.mongodb.*;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.bson.types.*;

public class BSONSerializerFactory {

    public BSONSerializerFactory() {
    }

    public static BSONObjectSerializer buildLegacyBSONSerializer() {
        
        BSONObjectSerializer serializer = new BSONObjectSerializer();
        
        serializer.addObjectSerializer(byte[].class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append("<Binary Data>");
            }

        });
        serializer.addObjectSerializer(Object[].class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append("[ ");
                for (int i = 0; i < Array.getLength(obj); i++) {
                    if (i > 0)
                        buf.append(" , ");
                    serializer.serialize(Array.get(obj, i), buf);
                }

                buf.append("]");
            }

        });

        serializer.addObjectSerializer(Binary.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append("<Binary Data>");
            }

        });

        serializer.addObjectSerializer(Boolean.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append(obj.toString());
            }

        });

        serializer.addObjectSerializer(BSONTimestamp.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                BSONTimestamp t = (BSONTimestamp) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$ts", Integer.valueOf(t.getTime()));
                temp.put("$inc", Integer.valueOf(t.getInc()));
                serializer.serialize(temp, buf);
            }

        });

        serializer.addObjectSerializer(byte[].class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append("<Binary Data>");
            }

        });

        serializer.addObjectSerializer(Code.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                Code c = (Code) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$code", c.getCode());
                serializer.serialize(temp, buf);
            }

        });

        serializer.addObjectSerializer(CodeWScope.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                CodeWScope c = (CodeWScope) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$code", c.getCode());
                temp.put("$scope", c.getScope());
                serializer.serialize(temp, buf);
            }

        });

        serializer.addObjectSerializer(Date.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                Date d = (Date) obj;
                SimpleDateFormat format = new SimpleDateFormat(
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                format.setCalendar(new GregorianCalendar(
                        new SimpleTimeZone(0, "GMT")));
                serializer.serialize(
                        new BasicDBObject("$date", format.format(d)),
                        buf);
            }

        });

        serializer.addObjectSerializer(DBObject.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                boolean first = true;
                buf.append("{ ");
                DBObject dbo = (DBObject) obj;
                String name;
                
                Iterator<String> it = dbo.keySet().iterator();
                while (it.hasNext() ){
                    name = it.next();
                    
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

        });

        serializer.addObjectSerializer(DBRefBase.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                DBRefBase ref = (DBRefBase) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$ref", ref.getRef());
                temp.put("$id", ref.getId());
                serializer.serialize(temp, buf);
            }

        });

        serializer.addObjectSerializer(Iterable.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                boolean first = true;
                buf.append("[ ");
                
                Iterator it = ((Iterable) obj).iterator();
                while ( it.hasNext() ) {
                    if (first)
                        first = false;
                    else
                        buf.append(" , ");
                    
                    serializer.serialize(it.next(), buf);
                }
                buf.append("]");
            }
        });

        serializer.addObjectSerializer(Map.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                boolean first = true;
                buf.append("{ ");
                Map m = (Map) obj;
                Entry entry;
                Iterator it = m.entrySet().iterator();
                
                while ( it.hasNext()) {
                    entry = (Entry) it.next();
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

        });

        serializer.addObjectSerializer(MaxKey.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                serializer.serialize(new BasicDBObject("$maxKey",
                        Integer.valueOf(1)), buf);
            }

        });
        serializer.addObjectSerializer(MinKey.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                serializer.serialize(new BasicDBObject("$minKey",
                        Integer.valueOf(1)), buf);
            }

        });

        serializer.addObjectSerializer(Number.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                buf.append(obj.toString());
            }

        });

        serializer.addObjectSerializer(ObjectId.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                serializer.serialize(
                        new BasicDBObject("$oid", obj.toString()), buf);
            }
        });
        
        serializer.addObjectSerializer(Pattern.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                DBObject externalForm = new BasicDBObject();
                externalForm.put("$regex", obj.toString());
                if (((Pattern) obj).flags() != 0)
                    externalForm.put("$options",
                            Bytes.regexFlags(((Pattern) obj).flags()));
                serializer.serialize(externalForm, buf);
            }
        });

        serializer.addObjectSerializer(String.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                JSON.string(buf, (String) obj);
            }
        });

        serializer.addObjectSerializer(UUID.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                UUID uuid = (UUID) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$uuid", uuid.toString());
                serializer.serialize(temp, buf);
            }
        });
        return serializer;
    }

    public static BSONObjectSerializer buildStrictBSONSerializer() {
        
        BSONObjectSerializer serializer = buildLegacyBSONSerializer();
        
        serializer.addObjectSerializer(Binary.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                Binary bin = (Binary) obj;
                DBObject temp = new BasicDBObject();
                temp.put("$binary",
                        (new Base64Codec()).encode(bin.getData()));
                temp.put("$type", Byte.valueOf(bin.getType()));
                serializer.serialize(temp, buf);
            }

        });

        serializer.addObjectSerializer(BSONTimestamp.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                BSONTimestamp t = (BSONTimestamp) obj;
                BasicDBObject temp = new BasicDBObject();
                temp.put("$t", Integer.valueOf(t.getTime()));
                temp.put("$i", Integer.valueOf(t.getInc()));
                BasicDBObject timestampObj = new BasicDBObject();
                timestampObj.put("$timestamp", temp);
                serializer.serialize(timestampObj, buf);
            }

        });

        serializer.addObjectSerializer(byte[].class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                DBObject temp = new BasicDBObject();
                temp.put("$binary", (new Base64Codec()).encode((byte[]) obj));
                temp.put("$type", Byte.valueOf((byte) 0));
                serializer.serialize(temp, buf);
            }
        });

        serializer.addObjectSerializer(Date.class,
                new BSONObjectSerializer.ObjectSerializer() {

            @Override
            public void serialize(Object obj,
                    BSONObjectSerializer serializer, StringBuilder buf) {
                Date d = (Date) obj;
                serializer.serialize(
                        new BasicDBObject("$date", Long.valueOf(d.getTime())), buf);
            }

        });

        return serializer;
    }
}
