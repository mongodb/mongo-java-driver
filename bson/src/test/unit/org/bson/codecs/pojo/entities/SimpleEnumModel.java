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

public final class SimpleEnumModel {

    private SimpleEnum myEnum;

    public SimpleEnumModel() {
    }

    public SimpleEnumModel(final SimpleEnum myEnum) {
        this.myEnum = myEnum;
    }

    /**
     * Returns the myEnum
     *
     * @return the myEnum
     */
    public SimpleEnum getMyEnum() {
        return myEnum;
    }

    /**
     * Sets the myEnum
     *
     * @param myEnum the myEnum
     * @return this
     */
    public SimpleEnumModel myEnum(final SimpleEnum myEnum) {
        this.myEnum = myEnum;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleEnumModel that = (SimpleEnumModel) o;

        if (getMyEnum() != that.getMyEnum()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getMyEnum() != null ? getMyEnum().hashCode() : 0;
    }
}
