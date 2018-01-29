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

import org.bson.codecs.pojo.annotations.BsonIgnore;

public final class AsymmetricalIgnoreModel {
    @BsonIgnore
    private String propertyIgnored;

    private String getterIgnored;

    private String setterIgnored;

    private String getterAndSetterIgnored;

    public AsymmetricalIgnoreModel() {
    }

    public AsymmetricalIgnoreModel(final String propertyIgnored, final String getterIgnored, final String setterIgnored,
                                   final String getterAndSetterIgnored) {
        this.propertyIgnored = propertyIgnored;
        this.getterIgnored = getterIgnored;
        this.setterIgnored = setterIgnored;
        this.getterAndSetterIgnored = getterAndSetterIgnored;
    }

    public String getPropertyIgnored() {
        return propertyIgnored;
    }

    public void setPropertyIgnored(final String propertyIgnored) {
        this.propertyIgnored = propertyIgnored;
    }

    @BsonIgnore
    public String getGetterIgnored() {
        return getterIgnored;
    }

    public void setGetterIgnored(final String getterIgnored) {
        this.getterIgnored = getterIgnored;
    }

    public String getSetterIgnored() {
        return setterIgnored;
    }

    @BsonIgnore
    public void setSetterIgnored(final String setterIgnored) {
        this.setterIgnored = setterIgnored;
    }

    @BsonIgnore
    public String getGetterAndSetterIgnored() {
        return getterAndSetterIgnored;
    }

    @BsonIgnore
    public void setGetterAndSetterIgnored(final String getterAndSetterIgnored) {
        this.getterAndSetterIgnored = getterAndSetterIgnored;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AsymmetricalIgnoreModel that = (AsymmetricalIgnoreModel) o;

        if (getPropertyIgnored() != null ? !getPropertyIgnored().equals(that.getPropertyIgnored()) : that.getPropertyIgnored() != null) {
            return false;
        }
        if (getGetterIgnored() != null ? !getGetterIgnored().equals(that.getGetterIgnored()) : that.getGetterIgnored() != null) {
            return false;
        }
        if (getSetterIgnored() != null ? !getSetterIgnored().equals(that.getSetterIgnored()) : that.getSetterIgnored() != null) {
            return false;
        }
        if (getGetterAndSetterIgnored() != null ? !getGetterAndSetterIgnored().equals(that.getGetterAndSetterIgnored()) : that
                .getGetterAndSetterIgnored() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getPropertyIgnored() != null ? getPropertyIgnored().hashCode() : 0;
        result = 31 * result + (getGetterIgnored() != null ? getGetterIgnored().hashCode() : 0);
        result = 31 * result + (getSetterIgnored() != null ? getSetterIgnored().hashCode() : 0);
        result = 31 * result + (getGetterAndSetterIgnored() != null ? getGetterAndSetterIgnored().hashCode() : 0);
        return result;
    }
}
