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

package com.mongodb.client.model;

import com.mongodb.MongoNamespace;

import java.util.ArrayList;
import java.util.List;

public class ClientWriteModelList {
    private final List<WriteModel<?>> writeModels;

    private final List<NamespaceRange> namespaceRanges;

    public static Builder builder() {
        return new Builder();
    }

    public List<WriteModel<?>> getWriteModels() {
        return writeModels;
    }

    public List<NamespaceRange> getNamespaceRanges() {
        return namespaceRanges;
    }

    public static class NamespaceRange {
        private final MongoNamespace namespace;
        private final int startIndex;
        private final int size;


        public MongoNamespace getNamespace() {
            return namespace;
        }

        public int getStartIndex() {
            return startIndex;
        }

        public int getSize() {
            return size;
        }

        public NamespaceRange(MongoNamespace namespace, int startIndex, int size) {
            this.namespace = namespace;
            this.startIndex = startIndex;
            this.size = size;
        }
    }

    public static class Builder {
        private final List<NamespaceRange> namespaceRanges = new ArrayList<>();
        private final List<WriteModel<?>> writeModels = new ArrayList<>();

        public <TDocument> Builder add(MongoNamespace namespace, WriteModel<TDocument> writeModel) {
            namespaceRanges.add(new NamespaceRange(namespace, this.writeModels.size(), 1));
            this.writeModels.add(writeModel);
            return this;
        }

        public <TDocument> Builder add(MongoNamespace namespace, List<WriteModel<TDocument>> writeModels) {
            namespaceRanges.add(new NamespaceRange(namespace, this.writeModels.size(), writeModels.size()));
            this.writeModels.addAll(writeModels);
            return this;
        }

        public ClientWriteModelList build() {
            return new ClientWriteModelList(this);
        }
    }

    public ClientWriteModelList(Builder builder) {
        this.writeModels = builder.writeModels;
        this.namespaceRanges = builder.namespaceRanges;
    }
}
