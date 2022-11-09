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

import java.util.Objects;

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

    public T getT() {
        return t;
    }

    public void setT(final T t) {
        this.t = t;
    }

    public V getV() {
        return v;
    }

    public void setV(final V v) {
        this.v = v;
    }

    public SelfReferentialGenericModel<V, T> getChild() {
        return child;
    }

    public void setChild(final SelfReferentialGenericModel<V, T> child) {
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

        if (!Objects.equals(t, that.t)) {
            return false;
        }
        if (!Objects.equals(v, that.v)) {
            return false;
        }
        if (!Objects.equals(child, that.child)) {
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
