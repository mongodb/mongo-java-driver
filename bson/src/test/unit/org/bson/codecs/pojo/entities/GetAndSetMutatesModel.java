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

public final class GetAndSetMutatesModel {
    private String fieldOne;
    private String fieldTwo;

    public GetAndSetMutatesModel(){
    }

    public GetAndSetMutatesModel(final String fieldOne, final String fieldTwo) {
        this.fieldOne = fieldOne;
        this.fieldTwo = fieldTwo;
    }

    public String getFieldOne() {
        return "get" + fieldOne;
    }

    public void setFieldOne(final String fieldOne) {
        this.fieldOne = "set" + fieldOne;
    }

    public String getFieldTwo() {
        return "get" + fieldTwo;
    }

    public void setFieldTwo(final String fieldTwo) {
        this.fieldTwo = "set" + fieldTwo;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetAndSetMutatesModel that = (GetAndSetMutatesModel) o;
        return Objects.equals(fieldOne, that.fieldOne)
                && Objects.equals(fieldTwo, that.fieldTwo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldOne, fieldTwo);
    }

    @Override
    public String toString() {
        return "GetAndSetMutatesModel{"
                + "fieldOne='" + fieldOne + '\''
                + ", fieldTwo='" + fieldTwo + '\''
                + '}';
    }
}
