/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;

@BsonDiscriminator(value = "MyAnnotationModel", key = "_cls")
public final class AnnotationWithObjectIdModel {

    @BsonId()
    public ObjectId customId;

    @BsonProperty(useDiscriminator = false)
    public AnnotationWithObjectIdModel child;

    @BsonProperty("renamed")
    public AnnotationWithObjectIdModel alternative;

    public AnnotationWithObjectIdModel() {
    }

    public AnnotationWithObjectIdModel(final ObjectId customId, final AnnotationWithObjectIdModel child,
                                       final AnnotationWithObjectIdModel alternative) {
        this.customId = customId;
        this.child = child;
        this.alternative = alternative;
    }

    public ObjectId getCustomId() {
        return customId;
    }

    public void setCustomId(final ObjectId customId) {
        this.customId = customId;
    }

    public AnnotationWithObjectIdModel getChild() {
        return child;
    }

    public void setChild(final AnnotationWithObjectIdModel child) {
        this.child = child;
    }

    public AnnotationWithObjectIdModel getAlternative() {
        return alternative;
    }

    public void setAlternative(final AnnotationWithObjectIdModel alternative) {
        this.alternative = alternative;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnnotationWithObjectIdModel that = (AnnotationWithObjectIdModel) o;

        if (getCustomId() != null ? !getCustomId().equals(that.getCustomId()) : that.getCustomId() != null) {
            return false;
        }
        if (getChild() != null ? !getChild().equals(that.getChild()) : that.getChild() != null) {
            return false;
        }
        if (getAlternative() != null ? !getAlternative().equals(that.getAlternative()) : that.getAlternative() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getCustomId() != null ? getCustomId().hashCode() : 0;
        result = 31 * result + (getChild() != null ? getChild().hashCode() : 0);
        result = 31 * result + (getAlternative() != null ? getAlternative().hashCode() : 0);
        return result;
    }
}
