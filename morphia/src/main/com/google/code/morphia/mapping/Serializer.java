/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping;

import org.bson.types.Binary;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class Serializer {
    /**
     * serializes object to byte[]
     */
    public static byte[] serialize(final Object o, final boolean zip) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os = baos;
        if (zip) {
            os = new GZIPOutputStream(os);
        }
        final ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(o);
        oos.flush();
        oos.close();

        return baos.toByteArray();
    }

    /**
     * deserializes DBBinary/byte[] to object
     */
    public static Object deserialize(final Object data, final boolean zipped) throws IOException,
                                                                                     ClassNotFoundException {
        final ByteArrayInputStream bais;
        if (data instanceof Binary) {
            bais = new ByteArrayInputStream(((Binary) data).getData());
        }
        else {
            bais = new ByteArrayInputStream((byte[]) data);
        }

        InputStream is = bais;
        try {
            if (zipped) {
                is = new GZIPInputStream(is);
            }

            final ObjectInputStream ois = new ObjectInputStream(is);
            return ois.readObject();
        } finally {
            is.close();
        }
    }

}
