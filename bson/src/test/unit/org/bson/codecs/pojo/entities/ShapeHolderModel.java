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

public final class ShapeHolderModel {

    private ShapeModelAbstract shape;

    public ShapeHolderModel() {
    }

    public ShapeHolderModel(final ShapeModelAbstract shape) {
        this.shape = shape;
    }

    public ShapeModelAbstract getShape() {
        return shape;
    }

    public void setShape(final ShapeModelAbstract shape) {
        this.shape = shape;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShapeHolderModel)) {
            return false;
        }

        ShapeHolderModel that = (ShapeHolderModel) o;

        if (getShape() != null ? !getShape().equals(that.getShape()) : that.getShape() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getShape() != null ? getShape().hashCode() : 0;
    }
}

