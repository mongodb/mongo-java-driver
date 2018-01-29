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

public final class SimpleNestedPojoModel {
    private SimpleModel simple;

    public SimpleNestedPojoModel() {
    }

    public SimpleNestedPojoModel(final SimpleModel simple) {
        this.simple = simple;
    }

    public SimpleModel getSimple() {
        return simple;
    }

    public void setSimple(final SimpleModel simple) {
        this.simple = simple;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleNestedPojoModel that = (SimpleNestedPojoModel) o;

        if (getSimple() != null ? !getSimple().equals(that.getSimple()) : that.getSimple() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getSimple() != null ? getSimple().hashCode() : 0;
    }
}
