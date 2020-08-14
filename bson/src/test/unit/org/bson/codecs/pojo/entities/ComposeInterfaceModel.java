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

import java.util.Objects;

public class ComposeInterfaceModel {
    private String title;
    private InterfaceModelB nestedModel;

    public ComposeInterfaceModel() {
    }

    public ComposeInterfaceModel(final String title, final InterfaceModelB nestedModel) {
        this.title = title;
        this.nestedModel = nestedModel;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(final String title) {
        this.title = title;
    }

    public InterfaceModelB getNestedModel() {
        return nestedModel;
    }

    public void setNestedModel(final InterfaceModelB nestedModel) {
        this.nestedModel = nestedModel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComposeInterfaceModel that = (ComposeInterfaceModel) o;
        return Objects.equals(title, that.title)
                && Objects.equals(nestedModel, that.nestedModel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, nestedModel);
    }

    @Override
    public String toString() {
        return "ComposeInterfaceModel{"
                + "title='" + title + '\''
                + ", nestedModel=" + nestedModel
                + '}';
    }
}
