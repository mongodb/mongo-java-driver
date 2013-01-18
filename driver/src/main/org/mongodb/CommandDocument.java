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

package org.mongodb;

import org.bson.types.Document;

// TODO: This probably should not subclass QueryFilterDocument, since it's not... a query filter
// Did it this way so that I can pass it to MongoQueryMessage constructor
public class CommandDocument extends QueryFilterDocument implements ConvertibleToDocument {
    private static final long serialVersionUID = -986632617844878612L;

    public CommandDocument() {
    }

    public CommandDocument(final String key, final Object value) {
        super(key, value);
    }

    @Override
    public Document toDocument() {
        return this;
    }
}
