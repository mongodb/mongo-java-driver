/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;


import org.mongodb.serialization.PrimitiveSerializers;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
public interface MongoClient extends Closeable {
    /**
     *
     * @param name
     * @return
     */
    MongoDatabase getDatabase(String name);

    /**
     *
     * @return operations over this client
     */
    MongoOperations getOperations();    // TODO: I think we should get rid of this.   It's at the wrong level of abstraction.

    /**
     * Run the given Runnable in the scope of a single connection.
     *
     * @param runnable what to do with the connection
     */
    void withConnection(Runnable runnable);

    /**
     * Run the given Callable in the scope of a single connection.
     *
     * @param callable what to do with the connection
     */
    <T> T withConnection(final Callable<T> callable) throws ExecutionException;

    /**
     *
     */
    void close();

    /**
     *
     * @return
     */
    WriteConcern getWriteConcern();

    /**
     *
     * @return
     */
    ReadPreference getReadPreference();

    PrimitiveSerializers getPrimitiveSerializers();
}
