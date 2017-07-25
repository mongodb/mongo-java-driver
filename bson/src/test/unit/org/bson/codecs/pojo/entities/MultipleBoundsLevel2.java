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

import java.util.List;
import java.util.Map;

public class MultipleBoundsLevel2<T> extends MultipleBoundsLevel3<String> {
    private List<T> level2;

    public MultipleBoundsLevel2() {
        super();
    }

    public MultipleBoundsLevel2(final Map<String, String> level3, final List<T> level2) {
        super(level3);
        this.level2 = level2;
    }

    public List<T> getLevel2() {
        return level2;
    }

    public void setLevel2(final List<T> level2) {
        this.level2 = level2;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MultipleBoundsLevel2<?> that = (MultipleBoundsLevel2<?>) o;

        if (level2 != null ? !level2.equals(that.level2) : that.level2 != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (level2 != null ? level2.hashCode() : 0);
        return result;
    }
}
