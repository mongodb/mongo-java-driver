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

package com.mongodb.acceptancetest.crud.pojo;

public class Postcode {
    private String postcode;

    public Postcode(final String postcode) {
        this.postcode = postcode;
    }

    public Postcode() {
        //no-args constructor required for decoding from database
    }

    // *** Standard Boilerplate
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return postcode.equals(((Postcode) o).postcode);
    }

    @Override
    public int hashCode() {
        return postcode.hashCode();
    }

    @Override
    public String toString() {
        return "Postcode{postcode='" + postcode + '\'' + '}';
    }
}
