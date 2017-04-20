/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

public class UpperBoundsModel<T extends Number> {
    private T myGenericField;

    public UpperBoundsModel() {
    }

    public UpperBoundsModel(final T myGenericField) {
        this.myGenericField = myGenericField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpperBoundsModel<?> that = (UpperBoundsModel<?>) o;

        if (myGenericField != null ? !myGenericField.equals(that.myGenericField) : that.myGenericField != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return myGenericField != null ? myGenericField.hashCode() : 0;
    }
}
