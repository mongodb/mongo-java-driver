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

public class Person {
    private String firstName;
    private String lastName;
    private final transient String ignoredValue = "ignoreMe!";

    //CHECKSTYLE:OFF
    //necessary evil until we do smart stuff around identifying the ID
    private String _id;
    //CHECKSTYLE:ON

    public Person(final String firstName, final String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public Person() {
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    // *** boilerplate required for tests

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Person person = (Person) o;

        return !(_id != null ? !_id.equals(person._id) : person._id != null)
               && firstName.equals(person.firstName)
               && lastName.equals(person.lastName);
    }

    @Override
    public int hashCode() {
        int result = firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + (_id != null ? _id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Person{"
               + "firstName='" + firstName + '\''
               + ", lastName='" + lastName + '\''
               + ", _id='" + _id + '\''
               + '}';
    }
}
