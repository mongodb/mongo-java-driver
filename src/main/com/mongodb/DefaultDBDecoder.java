/**
 *      Copyright (C) 2008 10gen Inc.
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
package com.mongodb;

import org.bson.BasicBSONDecoder;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author antoine
 */
public class DefaultDBDecoder extends BasicBSONDecoder implements DBDecoder {

    static class DefaultFactory implements DBDecoderFactory {
        @Override
        public DBDecoder create( ){
            return new DefaultDBDecoder( );
        }

        @Override
        public String toString() {
            return "DefaultDBDecoder.DefaultFactory";
        }
    }

    public static DBDecoderFactory FACTORY = new DefaultFactory();

    public DefaultDBDecoder( ){
    }
        
    public DBCallback getDBCallback(DBCollection collection) {
        // brand new callback every time
        return new DefaultDBCallback(collection);
    }

    public DBObject decode(byte[] b, DBCollection collection) {
        DBCallback cbk = getDBCallback(collection);
        cbk.reset();
        decode(b, cbk);
        return (DBObject) cbk.get();
    }

    public DBObject decode(InputStream in,  DBCollection collection) throws IOException {
        DBCallback cbk = getDBCallback(collection);
        cbk.reset();
        decode(in, cbk);
        return (DBObject) cbk.get();
    }

    @Override
    public String toString() {
        return "DefaultDBDecoder";
    }
}
