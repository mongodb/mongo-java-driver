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

public final class NestedReusedGenericsModel {

    private ReusedGenericsModel<Long, List<SimpleModel>, String> nested;

    public NestedReusedGenericsModel() {
    }

    public NestedReusedGenericsModel(final ReusedGenericsModel<Long, List<SimpleModel>, String> nested) {
        this.nested = nested;
    }

    /**
     * Returns the nested
     *
     * @return the nested
     */
    public ReusedGenericsModel<Long, List<SimpleModel>, String> getNested() {
        return nested;
    }

    public void setNested(final ReusedGenericsModel<Long, List<SimpleModel>, String> nested) {
        this.nested = nested;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NestedReusedGenericsModel)) {
            return false;
        }

        NestedReusedGenericsModel that = (NestedReusedGenericsModel) o;

        if (getNested() != null ? !getNested().equals(that.getNested()) : that.getNested() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getNested() != null ? getNested().hashCode() : 0;
    }

    @Override
    public String toString() {
        return "NestedReusedGenericsModel{"
                + "nested=" + nested
                + "}";
    }
}
