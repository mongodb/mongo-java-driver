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

import org.bson.codecs.pojo.annotations.BsonIgnore;

public final class PropertySelectionModel {
    private String stringField = "stringField";

    private final String finalStringField = "finalStringField";

    @BsonIgnore
    private String ignoredStringField = "ignoreMe";

    private String anotherIgnoredStringField = "ignoreMe";

    private static final String staticFinalStringField = "staticFinalStringField";

    private static String staticStringField = "staticStringField";

    private transient String transientString = "transientString";

    public PropertySelectionModel() {
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    public String getFinalStringField() {
        return finalStringField;
    }

    public void setFinalStringField(final String finalStringField) {
        throw new IllegalStateException("Not allowed");
    }

    public static String getStaticFinalStringField() {
        return staticFinalStringField;
    }

    public void setStaticFinalStringField(final String staticFinalStringField) {
        throw new IllegalStateException("Not allowed");
    }

    public static String getStaticStringField() {
        return staticStringField;
    }

    public static void setStaticStringField(final String staticStringField) {
        throw new IllegalStateException("Not allowed");
    }

    public String getTransientString() {
        return transientString;
    }

    public void setTransientString(final String transientString) {
        throw new IllegalStateException("Not allowed");
    }

    public String getIgnoredStringField() {
        return ignoredStringField;
    }

    public void setIgnoredStringField(final String ignoredStringField) {
        this.ignoredStringField = ignoredStringField;
    }

    @BsonIgnore
    public String getAnotherIgnoredStringField() {
        return anotherIgnoredStringField;
    }

    @BsonIgnore
    public void setAnotherIgnoredStringField(final String anotherIgnoredStringField) {
        this.anotherIgnoredStringField = anotherIgnoredStringField;
    }

    public int getfoo() {
        return 42;
    }

    public void setfoo(final int foo) {
    }

    public void is() {
    }

    public void isfoo() {
    }

    public int get() {
        return 42;
    }

    public void set(final int foo) {
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PropertySelectionModel that = (PropertySelectionModel) o;

        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }
        if (getFinalStringField() != null ? !getFinalStringField().equals(that.getFinalStringField())
                : that.getFinalStringField() != null) {
            return false;
        }
        if (getTransientString() != null ? !getTransientString().equals(that.getTransientString()) : that.getTransientString() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getStringField() != null ? getStringField().hashCode() : 0;
        result = 31 * result + (getFinalStringField() != null ? getFinalStringField().hashCode() : 0);
        result = 31 * result + (getTransientString() != null ? getTransientString().hashCode() : 0);
        return result;
    }
}
