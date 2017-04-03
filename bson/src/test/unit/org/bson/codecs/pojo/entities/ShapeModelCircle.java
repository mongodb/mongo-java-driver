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

public final class ShapeModelCircle extends ShapeModelAbstract {

    private Double radius;

    public ShapeModelCircle() {
    }

    public ShapeModelCircle(final String color, final Double radius) {
        super(color);
        this.radius = radius;
    }

    public Double getRadius() {
        return radius;
    }

    public void setRadius(final Double radius) {
        this.radius = radius;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShapeModelCircle)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ShapeModelCircle that = (ShapeModelCircle) o;

        if (getRadius() != null ? !getRadius().equals(that.getRadius()) : that.getRadius() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getRadius() != null ? getRadius().hashCode() : 0);
        return result;
    }
}
