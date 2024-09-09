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

abstract class AbstractClientNamespacedWriteModel {
    private final MongoNamespace namespace;
    private final ClientWriteModel model;

    AbstractClientNamespacedWriteModel(final MongoNamespace namespace, final ClientWriteModel model) {
        this.namespace = namespace;
        this.model = model;
    }

    public final MongoNamespace getNamespace() {
        return namespace;
    }

    public final ClientWriteModel getModel() {
        return model;
    }

    @Override
    public final String toString() {
        return "ClientNamespacedWriteModel{"
                + "namespace=" + namespace
                + ", model=" + model
                + '}';
    }
}
