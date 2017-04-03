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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestedSelfReferentialGenericModel<?, ?, ?> that = (NestedSelfReferentialGenericModel<?, ?, ?>) o;

        if (t != null ? !t.equals(that.t) : that.t != null) {
            return false;
        }
        if (v != null ? !v.equals(that.v) : that.v != null) {
            return false;
        }
        if (z != null ? !z.equals(that.z) : that.z != null) {
            return false;
        }
        if (selfRef1 != null ? !selfRef1.equals(that.selfRef1) : that.selfRef1 != null) {
            return false;
        }
        if (selfRef2 != null ? !selfRef2.equals(that.selfRef2) : that.selfRef2 != null) {
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
