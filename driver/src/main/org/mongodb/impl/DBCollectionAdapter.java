/**
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
 *
 */

package org.mongodb.impl;

//import com.mongodb.CommandResult;
//import com.mongodb.DBDecoder;
//import com.mongodb.DBEncoder;
//import com.mongodb.DBObject;
//import com.mongodb.ReadPreference;
//import com.mongodb.WriteConcern;
//import com.mongodb.WriteResult;
//import org.mongodb.Collection;
//import org.mongodb.DropCollectionCommand;
//
//import java.util.Iterator;
//import java.util.List;
//
///**
// * DBCollection adapter to new API.  THIS CLASS SHOULD NOT BE CONSIDERED PART OF THE PUBLIC API.
// */
//public class DBCollectionAdapter {
//    private final CollectionImpl impl;
//
//    public DBCollectionAdapter(final CollectionImpl impl) {
//        this.impl = impl;
//    }
//
//    public Collection getCollection() {
//        return impl;
//    }
//
//    public WriteResult insert(final List<DBObject> list, final WriteConcern concern, final DBEncoder encoder) {
//        throw new UnsupportedOperationException();
//    }
//
//    public WriteResult update(final DBObject q, final DBObject o, final boolean upsert, final boolean multi,
//                              final WriteConcern concern, final DBEncoder encoder) {
//        throw new UnsupportedOperationException();
//    }
//
//    public WriteResult remove(final DBObject o, final WriteConcern concern, final DBEncoder encoder) {
//        throw new UnsupportedOperationException();
//    }
//
//    public Iterator<DBObject> find(final DBObject ref, final DBObject fields, final int numToSkip, final int batchSize,
//                                   final int limit, final int options, final ReadPreference readPref, final DBDecoder decoder, final DBEncoder encoder) {
//        throw new UnsupportedOperationException();
//    }
//
//    public void createIndex(final DBObject keys, final DBObject options, final DBEncoder encoder) {
//        throw new UnsupportedClassVersionError();
//    }
//
//    public void drop() {
//        CommandResult res = new DropCollectionCommand(impl).execute();
//        if (!res.ok() && !res.getErrorMessage().equals("ns not found")) {
//            res.throwOnError();
//        }
//    }
//}
