/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

public class Person {
    private Address address;
    private Name name;

    public Person(final Address address, final Name name) {
        this.address = address;
        this.name = name;
    }

    public Person() {
        //needed for Pojo serialisation
    }

    //**** Really simple POJO needs all this boilerplate just to make testing easy/correct
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Person person = (Person) o;

        return address.equals(person.address) && name.equals(person.name);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Person{"
               + "address=" + address
               + ", name=" + name
               + '}';
    }
}
