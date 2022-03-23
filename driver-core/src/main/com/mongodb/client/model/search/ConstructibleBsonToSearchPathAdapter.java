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
package com.mongodb.client.model.search;

import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.notNull;

final class ConstructibleBsonToSearchPathAdapter extends AbstractConstructibleBson<ConstructibleBsonToSearchPathAdapter>
        implements FieldSearchPath, WildcardSearchPath {
    static final String VALUE_KEY = "value";

    ConstructibleBsonToSearchPathAdapter(final BsonDocument doc) {
        super(assertNotEmpty(doc));
    }

    @Override
    protected ConstructibleBsonToSearchPathAdapter newSelf(final BsonDocument doc) {
        return new ConstructibleBsonToSearchPathAdapter(doc);
    }

    @Override
    public FieldSearchPath multi(final String analyzerName) {
        return newAppended("multi", new BsonString(notNull("analyzerName", analyzerName)));
    }

    @Override
    public BsonValue toBsonValue() {
        final BsonDocument doc = toBsonDocument();
        if (doc.size() > 1) {
            return doc;
        } else {
            final BsonString value = doc.getString(VALUE_KEY, null);
            if (value != null) {
                // paths that contain only `VALUE_KEY` can be represented as a `BsonString`
                return value;
            } else {
                return doc;
            }
        }
    }

    private static BsonDocument assertNotEmpty(final BsonDocument doc) {
        assertFalse(doc.isEmpty());
        return doc;
    }
}
