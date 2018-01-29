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

public class GenericHolderModel<P> {

    private P myGenericField;
    private Long myLongField;

    public GenericHolderModel() {
    }

    public GenericHolderModel(final P myGenericField, final Long myLongField) {
        this.myGenericField = myGenericField;
        this.myLongField = myLongField;
    }

    public P getMyGenericField() {
        return myGenericField;
    }

    public void setMyGenericField(final P myGenericField) {
        this.myGenericField = myGenericField;
    }

    public Long getMyLongField() {
        return myLongField;
    }

    public void setMyLongField(final Long myLongField) {
        this.myLongField = myLongField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericHolderModel)) {
            return false;
        }

        GenericHolderModel<?> that = (GenericHolderModel<?>) o;

        if (getMyGenericField() != null ? !getMyGenericField().equals(that.getMyGenericField()) : that.getMyGenericField() != null) {
            return false;
        }
        if (getMyLongField() != null ? !getMyLongField().equals(that.getMyLongField()) : that.getMyLongField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getMyGenericField() != null ? getMyGenericField().hashCode() : 0;
        result = 31 * result + (getMyLongField() != null ? getMyLongField().hashCode() : 0);
        return result;
    }
}
