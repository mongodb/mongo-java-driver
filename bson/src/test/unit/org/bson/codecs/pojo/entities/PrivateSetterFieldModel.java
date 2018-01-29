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

public final class PrivateSetterFieldModel {

    private Integer integerField;
    private String stringField;
    private List<String> listField;

    public PrivateSetterFieldModel(){
    }

    public PrivateSetterFieldModel(final Integer integerField, final String stringField, final List<String> listField) {
        this.integerField = integerField;
        this.stringField = stringField;
        this.listField = listField;
    }

    public Integer getIntegerField() {
        return integerField;
    }

    public String getStringField() {
        return stringField;
    }

    public List<String> getListField() {
        return listField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrivateSetterFieldModel that = (PrivateSetterFieldModel) o;

        if (getIntegerField() != null ? !getIntegerField().equals(that.getIntegerField()) : that.getIntegerField() != null) {
            return false;
        }
        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }
        return getListField() != null ? getListField().equals(that.getListField()) : that.getListField() == null;
    }

    @Override
    public int hashCode() {
        int result = getIntegerField() != null ? getIntegerField().hashCode() : 0;
        result = 31 * result + (getStringField() != null ? getStringField().hashCode() : 0);
        result = 31 * result + (getListField() != null ? getListField().hashCode() : 0);
        return result;
    }
}
