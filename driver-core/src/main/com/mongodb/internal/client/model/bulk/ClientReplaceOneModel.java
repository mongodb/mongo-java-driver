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
import com.mongodb.client.model.bulk.ClientReplaceOptions;
import com.mongodb.client.model.bulk.ClientWriteModel;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ClientReplaceOneModel<T> implements ClientWriteModel<T> {
    private final MongoNamespace namespace;
    private final Bson filter;
    private final T replacement;
    private final ConcreteClientReplaceOptions options;

    public ClientReplaceOneModel(
            final MongoNamespace namespace,
            final Bson filter,
            final T replacement,
            @Nullable final ClientReplaceOptions options) {
        this.namespace = namespace;
        this.filter = filter;
        this.replacement = replacement;
        this.options = options == null ? ConcreteClientReplaceOptions.EMPTY : (ConcreteClientReplaceOptions) options;
    }

    @Override
    public String toString() {
        return "ClientReplaceOneModel{"
                + "namespace=" + namespace
                + ", filter=" + filter
                + ", replacement=" + replacement
                + ", options=" + options
                + '}';
    }
}
