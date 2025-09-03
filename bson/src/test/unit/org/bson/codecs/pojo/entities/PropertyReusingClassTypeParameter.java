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

public final class PropertyReusingClassTypeParameter<A> {

    public GenericTreeModel<A, A> tree;

    public PropertyReusingClassTypeParameter(){
    }

    public PropertyReusingClassTypeParameter(final GenericTreeModel<A, A> tree) {
        this.tree = tree;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PropertyReusingClassTypeParameter<?> that = (PropertyReusingClassTypeParameter<?>) o;

        if (!Objects.equals(tree, that.tree)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return tree != null ? tree.hashCode() : 0;
    }
}
