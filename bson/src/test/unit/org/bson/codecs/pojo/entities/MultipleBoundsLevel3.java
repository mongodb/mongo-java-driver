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

import java.util.Map;

public class MultipleBoundsLevel3<T> {
    private Map<String, T> level3;

    public MultipleBoundsLevel3() {
    }

    public MultipleBoundsLevel3(final Map<String, T> level3) {
        this.level3 = level3;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultipleBoundsLevel3<?> that = (MultipleBoundsLevel3<?>) o;

        if (level3 != null ? !level3.equals(that.level3) : that.level3 != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return level3 != null ? level3.hashCode() : 0;
    }
}
