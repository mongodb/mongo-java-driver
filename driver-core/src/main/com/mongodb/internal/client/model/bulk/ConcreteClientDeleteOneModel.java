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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.bulk.ClientDeleteOneModel;
import com.mongodb.client.model.bulk.ClientDeleteOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ConcreteClientDeleteOneModel implements ClientDeleteOneModel {
    private final MongoNamespace namespace;
    private final Bson filter;
    private final ConcreteClientDeleteOptions options;

    public ConcreteClientDeleteOneModel(
            final MongoNamespace namespace,
            final Bson filter,
            @Nullable final ClientDeleteOptions options) {
        this.namespace = namespace;
        this.filter = filter;
        this.options = options == null ? ConcreteClientDeleteOptions.MUTABLE_EMPTY : (ConcreteClientDeleteOptions) options;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public Bson getFilter() {
        return filter;
    }

    public ConcreteClientDeleteOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "ClientDeleteOneModel{"
                + "namespace=" + namespace
                + ", filter=" + filter
                + ", options=" + options
                + '}';
    }
}
