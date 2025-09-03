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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.Objects;

@BsonDiscriminator
public class InterfaceModelImplA implements InterfaceModel {
    private boolean value;
    private String name;

    public boolean isValue() {
        return value;
    }

    public InterfaceModelImplA setValue(final boolean value) {
        this.value = value;
        return this;
    }

    public String getName() {
        return name;
    }

    public InterfaceModelImplA setName(final String name) {
        this.name = name;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InterfaceModelImplA that = (InterfaceModelImplA) o;

        if (value != that.value) {
            return false;
        }
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        int result = (value ? 1 : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
