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

package tour;

import org.bson.types.ObjectId;

/**
 * The Person Pojo
 */
public final class Person {
    private ObjectId id;
    private String name;
    private int age;
    private Address address;

    /**
     * Construct a new instance
     */
    public Person() {
    }

    /**
     * Construct a new instance
     *
     * @param name the name
     * @param age the age
     * @param address the address
     */
    public Person(final String name, final int age, final Address address) {
        this.id = new ObjectId();
        this.name = name;
        this.age = age;
        this.address = address;
    }

    /**
     * Returns the id
     *
     * @return the id
     */
    public ObjectId getId() {
        return id;
    }

    /**
     * Sets the id
     *
     * @param id the id
     */
    public void setId(final ObjectId id) {
        this.id = id;
    }

    /**
     * Returns the name
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name
     *
     * @param name the name
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Returns the age
     *
     * @return the age
     */
    public int getAge() {
        return age;
    }

    /**
     * Sets the age
     *
     * @param age the age
     */
    public void setAge(final int age) {
        this.age = age;
    }

    /**
     * Returns the address
     *
     * @return the address
     */
    public Address getAddress() {
        return address;
    }

    /**
     * Sets the address
     *
     * @param address the address
     */
    public void setAddress(final Address address) {
        this.address = address;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Person person = (Person) o;

        if (getAge() != person.getAge()) {
            return false;
        }
        if (getId() != null ? !getId().equals(person.getId()) : person.getId() != null) {
            return false;
        }
        if (getName() != null ? !getName().equals(person.getName()) : person.getName() != null) {
            return false;
        }
        if (getAddress() != null ? !getAddress().equals(person.getAddress()) : person.getAddress() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + getAge();
        result = 31 * result + (getAddress() != null ? getAddress().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Person{"
                + "id='" + id + "'"
                + ", name='" + name + "'"
                + ", age=" + age
                + ", address=" + address
                + "}";
    }
}
