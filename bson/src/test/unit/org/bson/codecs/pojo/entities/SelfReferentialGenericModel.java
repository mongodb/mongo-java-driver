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

public final class SelfReferentialGenericModel<T, V> {
    private T t;
    private V v;
    private SelfReferentialGenericModel<V, T> child;

    public SelfReferentialGenericModel() {
    }

    public SelfReferentialGenericModel(final T t, final V v, final SelfReferentialGenericModel<V, T> child) {
        this.t = t;
        this.v = v;
        this.child = child;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SelfReferentialGenericModel<?, ?> that = (SelfReferentialGenericModel<?, ?>) o;

        if (t != null ? !t.equals(that.t) : that.t != null) {
            return false;
        }
        if (v != null ? !v.equals(that.v) : that.v != null) {
            return false;
        }
        if (child != null ? !child.equals(that.child) : that.child != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = t != null ? t.hashCode() : 0;
        result = 31 * result + (v != null ? v.hashCode() : 0);
        result = 31 * result + (child != null ? child.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SelfReferentialGenericModel{"
                + "t=" + t
                + ", v=" + v
                + ", child=" + child
                + "}";
    }
}
