/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

/**
 * An exception that indicates a logical error in the driver.
 */
public class MongoInternalException extends MongoException {
    /**
     * Construct a new instance with the given message and chained exception
     * @param msg the message
     * @param t  the chained exception
     */
    public MongoInternalException(final String msg, final Throwable t) {
        super(msg, t);
    }
}
