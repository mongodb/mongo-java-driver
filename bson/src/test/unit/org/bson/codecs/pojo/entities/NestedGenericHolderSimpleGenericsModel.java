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
import java.util.Map;

public final class NestedGenericHolderSimpleGenericsModel {
    private GenericHolderModel<SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>>> nested;

    public NestedGenericHolderSimpleGenericsModel() {
    }

    public NestedGenericHolderSimpleGenericsModel(
            final GenericHolderModel<SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>>> nested) {
        this.nested = nested;
    }

    public GenericHolderModel<SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>>> getNested() {
        return nested;
    }

    public void setNested(final GenericHolderModel<SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>>> nested) {
        this.nested = nested;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NestedGenericHolderSimpleGenericsModel)) {
            return false;
        }

        NestedGenericHolderSimpleGenericsModel that = (NestedGenericHolderSimpleGenericsModel) o;

        if (nested != null ? !nested.equals(that.nested) : that.nested != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return nested != null ? nested.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "NestedGenericHolderSimpleGenericsModel{"
                + "nested=" + nested
                + "}";
    }
}
