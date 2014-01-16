/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.codecs.pojo;

import java.util.List;

public class ListWrapper {
    private List<Integer> integerList;

    public ListWrapper(final List<Integer> integerList) {
        this.integerList = integerList;
    }

    public ListWrapper() {
    }

    //**** Boilerplate
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ListWrapper that = (ListWrapper) o;

        if (!integerList.equals(that.integerList)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return integerList.hashCode();
    }

    @Override
    public String toString() {
        return "ListWrapper{integerList=" + integerList + '}';
    }
}
