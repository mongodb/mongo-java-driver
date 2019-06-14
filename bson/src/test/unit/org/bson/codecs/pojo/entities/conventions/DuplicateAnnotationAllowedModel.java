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

package org.bson.codecs.pojo.entities.conventions;

import javax.annotation.Nullable;

public class DuplicateAnnotationAllowedModel {

    public String id;

    public DuplicateAnnotationAllowedModel() {

    }

    public DuplicateAnnotationAllowedModel(final String id) {
        this.id = id;
    }

    @Nullable
    public String optionalString;

    @Nullable
    public String getOptionalString() {
        return optionalString;
    }

    public void setOptionalString(@Nullable final String optionalString) {
        this.optionalString = optionalString;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DuplicateAnnotationAllowedModel that = (DuplicateAnnotationAllowedModel) o;

        return (id != null ? id.equals(that.id) : that.id == null)
               && (optionalString != null ? optionalString.equals(that.optionalString) : that.optionalString == null);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (optionalString != null ? optionalString.hashCode() : 0);
        return result;
    }
}
