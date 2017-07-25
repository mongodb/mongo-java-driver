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

public final class SimpleModel {
    private Integer integerField;
    private String stringField;

    public SimpleModel(){
    }

    public SimpleModel(final Integer integerField, final String stringField) {
        this.integerField = integerField;
        this.stringField = stringField;
    }

    public Integer getIntegerField() {
        return integerField;
    }

    public void setIntegerField(final Integer integerField) {
        this.integerField = integerField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleModel that = (SimpleModel) o;

        if (getIntegerField() != null ? !getIntegerField().equals(that.getIntegerField()) : that.getIntegerField() != null) {
            return false;
        }
        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getIntegerField() != null ? getIntegerField().hashCode() : 0;
        result = 31 * result + (getStringField() != null ? getStringField().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SimpleModel{"
                + "integerField=" + integerField
                + ", stringField='" + stringField + "'"
                + "}";
    }
}
