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

package org.bson.codecs.pojo.entities;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;
import java.util.Objects;

public class CollectionSpecificReturnTypeCreatorModel extends AbstractCollectionSpecificReturnTypeCreatorModel {
    private final ImmutableList<String> properties;

    @BsonCreator
    public CollectionSpecificReturnTypeCreatorModel(@BsonProperty("properties") final List<String> properties) {
        this.properties = ImmutableList.copyOf(properties);
    }

    public ImmutableList<String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CollectionSpecificReturnTypeCreatorModel that = (CollectionSpecificReturnTypeCreatorModel) o;

        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return properties != null ? properties.hashCode() : 0;
    }
}
