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

import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;


@BsonDiscriminator(value = "AnnotatedConventionModel", key = "_cls")
public final class ConventionModel {
    private static final int myStaticField = 10;
    private transient int myTransientField = 10;
    private final int myFinalField = 10;
    private int myIntField = 10;

    @BsonId()
    private String customId;

    @BsonProperty(useDiscriminator = false)
    private ConventionModel child;

    @BsonProperty(value = "model", useDiscriminator = false)
    private SimpleModel simpleModel;

    public ConventionModel(){
    }

    public ConventionModel(final String customId, final ConventionModel child, final SimpleModel simpleModel) {
        this.myIntField = myIntField;
        this.customId = customId;
        this.child = child;
        this.simpleModel = simpleModel;
    }

    public int getMyIntField() {
        return myIntField;
    }

    public void setMyIntField(final int myIntField) {
        this.myIntField = myIntField;
    }

    public int getMyFinalField() {
        return myFinalField;
    }

    public int getMyTransientField() {
        return myTransientField;
    }

    public void setMyTransientField(final int myTransientField) {
        this.myTransientField = myTransientField;
    }

    public String getCustomId() {
        return customId;
    }

    public void setCustomId(final String customId) {
        this.customId = customId;
    }

    public ConventionModel getChild() {
        return child;
    }

    public void setChild(final ConventionModel child) {
        this.child = child;
    }

    public SimpleModel getSimpleModel() {
        return simpleModel;
    }

    public void setSimpleModel(final SimpleModel simpleModel) {
        this.simpleModel = simpleModel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConventionModel that = (ConventionModel) o;

        if (getCustomId() != null ? !getCustomId().equals(that.getCustomId()) : that.getCustomId() != null) {
            return false;
        }
        if (getChild() != null ? !getChild().equals(that.getChild()) : that.getChild() != null) {
            return false;
        }
        if (getSimpleModel() != null ? !getSimpleModel().equals(that.getSimpleModel()) : that.getSimpleModel() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getCustomId() != null ? getCustomId().hashCode() : 0;
        result = 31 * result + (getChild() != null ? getChild().hashCode() : 0);
        result = 31 * result + (getSimpleModel() != null ? getSimpleModel().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConventionModel{"
                + "customId='" + customId + "'"
                + ", child=" + child
                + ", simpleModel=" + simpleModel
                + "}";
    }
}
