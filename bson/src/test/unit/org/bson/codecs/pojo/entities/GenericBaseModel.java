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

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.Objects;

@BsonDiscriminator()
public class GenericBaseModel<T extends BaseField> {

    private T field;

    public GenericBaseModel(final T field) {
        this.field = field;
    }

    public GenericBaseModel() {
    }

    public T getField() {
        return field;
    }

    public void setField(final T field) {
        this.field = field;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericBaseModel<?> that = (GenericBaseModel<?>) o;
        return Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field);
    }
}
