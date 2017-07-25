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

@BsonDiscriminator()
public abstract class ShapeModelAbstract {

    private String color;

    public ShapeModelAbstract() {
    }

    public ShapeModelAbstract(final String color) {
        this.color = color;
    }

    public String getColor() {
        return color;
    }

    public void setColor(final String color) {
        this.color = color;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShapeModelAbstract)) {
            return false;
        }

        ShapeModelAbstract that = (ShapeModelAbstract) o;

        if (getColor() != null ? !getColor().equals(that.getColor()) : that.getColor() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getColor() != null ? getColor().hashCode() : 0;
    }
}
