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
import com.mongodb.client.model.bulk.ClientUpdateOptions;
import com.mongodb.client.model.bulk.ClientWriteModel;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertTrue;
import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ClientUpdateOneModel implements ClientWriteModel {
    private final MongoNamespace namespace;
    private final Bson filter;
    @Nullable
    private final Bson update;
    @Nullable
    private final Iterable<? extends Bson> updatePipeline;
    private final ConcreteClientUpdateOptions options;

    public ClientUpdateOneModel(
            final MongoNamespace namespace,
            final Bson filter,
            @Nullable
            final Bson update,
            @Nullable
            final Iterable<? extends Bson> updatePipeline,
            @Nullable final ClientUpdateOptions options) {
        this.namespace = namespace;
        this.filter = filter;
        assertTrue(update == null ^ updatePipeline == null);
        this.update = update;
        this.updatePipeline = updatePipeline;
        this.options = options == null ? ConcreteClientUpdateOptions.MUTABLE_EMPTY : (ConcreteClientUpdateOptions) options;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public Bson getFilter() {
        return filter;
    }

    public Optional<Bson> getUpdate() {
        return ofNullable(update);
    }

    public Optional<Iterable<? extends Bson>> getUpdatePipeline() {
        return ofNullable(updatePipeline);
    }

    public ConcreteClientUpdateOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "ClientUpdateOneModel{"
                + "namespace=" + namespace
                + ", filter=" + filter
                + ", update=" + (update != null ? update : updatePipeline)
                + ", options=" + options
                + '}';
    }
}
