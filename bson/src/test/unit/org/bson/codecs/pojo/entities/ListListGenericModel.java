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

import java.util.List;
import java.util.Objects;

public class ListListGenericModel<T> {

    private List<List<T>> values;

    public ListListGenericModel() {
    }

    public ListListGenericModel(final List<List<T>> values) {
        this.values = values;
    }

    public List<List<T>> getValues() {
        return values;
    }

    public void setValues(final List<List<T>> values) {
        this.values = values;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ListListGenericModel<?> that = (ListListGenericModel<?>) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "ListListGenericModel{"
                + "values=" + values
                + '}';
    }
}
