/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs.configuration.mapper.conventions.entities;

public class ZipCode {
    private Integer number;
    private Integer extended;

    public ZipCode() {
    }

    public ZipCode(final Integer number, final Integer extended) {
        this.number = number;
        this.extended = extended;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(final Integer number) {
        this.number = number;
    }

    public Integer getExtended() {
        return extended;
    }

    public void setExtended(final Integer extended) {
        this.extended = extended;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ZipCode zipCode = (ZipCode) o;

        if (!number.equals(zipCode.number)) {
            return false;
        }
        return extended.equals(zipCode.extended);

    }

    @Override
    public int hashCode() {
        int result = number.hashCode();
        result = 31 * result + extended.hashCode();
        return result;
    }
}
