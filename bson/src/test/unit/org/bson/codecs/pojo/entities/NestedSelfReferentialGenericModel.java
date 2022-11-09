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

public final class NestedSelfReferentialGenericModel<T, V, Z> {
    private T t;
    private V v;
    private Z z;
    private SelfReferentialGenericModel<T, V> selfRef1;
    private SelfReferentialGenericModel<T, Z> selfRef2;

    public NestedSelfReferentialGenericModel() {
    }

    public NestedSelfReferentialGenericModel(final T t, final V v, final Z z, final SelfReferentialGenericModel<T, V> selfRef1,
                         final SelfReferentialGenericModel<T, Z> selfRef2) {
        this.t = t;
        this.v = v;
        this.z = z;
        this.selfRef1 = selfRef1;
        this.selfRef2 = selfRef2;
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

    public Z getZ() {
        return z;
    }

    public void setZ(final Z z) {
        this.z = z;
    }

    public SelfReferentialGenericModel<T, V> getSelfRef1() {
        return selfRef1;
    }

    public void setSelfRef1(final SelfReferentialGenericModel<T, V> selfRef1) {
        this.selfRef1 = selfRef1;
    }

    public SelfReferentialGenericModel<T, Z> getSelfRef2() {
        return selfRef2;
    }

    public void setSelfRef2(final SelfReferentialGenericModel<T, Z> selfRef2) {
        this.selfRef2 = selfRef2;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestedSelfReferentialGenericModel<?, ?, ?> that = (NestedSelfReferentialGenericModel<?, ?, ?>) o;

        if (!Objects.equals(t, that.t)) {
            return false;
        }
        if (!Objects.equals(v, that.v)) {
            return false;
        }
        if (!Objects.equals(z, that.z)) {
            return false;
        }
        if (!Objects.equals(selfRef1, that.selfRef1)) {
            return false;
        }
        if (!Objects.equals(selfRef2, that.selfRef2)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = t != null ? t.hashCode() : 0;
        result = 31 * result + (v != null ? v.hashCode() : 0);
        result = 31 * result + (z != null ? z.hashCode() : 0);
        result = 31 * result + (selfRef1 != null ? selfRef1.hashCode() : 0);
        result = 31 * result + (selfRef2 != null ? selfRef2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NestedSelfReferentialGenericModel{"
                + "t=" + t
                + ", v=" + v
                + ", z=" + z
                + ", selfRef1=" + selfRef1
                + ", selfRef2=" + selfRef2
                + "}";
    }
}
