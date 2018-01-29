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

public final class ShapeModelRectangle extends ShapeModelAbstract {

    private Double width;
    private Double height;

    public ShapeModelRectangle() {
    }

    public ShapeModelRectangle(final String color, final Double width, final Double height) {
        super(color);
        this.width = width;
        this.height = height;
    }

    public Double getWidth() {
        return width;
    }

    public void setWidth(final Double width) {
        this.width = width;
    }

    public Double getHeight() {
        return height;
    }

    public void setHeight(final Double height) {
        this.height = height;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShapeModelRectangle)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ShapeModelRectangle that = (ShapeModelRectangle) o;

        if (getWidth() != null ? !getWidth().equals(that.getWidth()) : that.getWidth() != null) {
            return false;
        }
        if (getHeight() != null ? !getHeight().equals(that.getHeight()) : that.getHeight() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getWidth() != null ? getWidth().hashCode() : 0);
        result = 31 * result + (getHeight() != null ? getHeight().hashCode() : 0);
        return result;
    }
}
