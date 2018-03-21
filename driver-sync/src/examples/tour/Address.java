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

/**
 * The Address POJO
 */
public final class Address {

    private String street;
    private String city;
    private String zip;

    /**
     * Construct a new instance
     */
    public Address() {
    }

    /**
     * Construct a new instance
     *
     * @param street the street
     * @param city the city
     * @param zip the zip / postal code
     */
    public Address(final String street, final String city, final String zip) {
        this.street = street;
        this.city = city;
        this.zip = zip;
    }

    /**
     * Returns the street
     *
     * @return the street
     */
    public String getStreet() {
        return street;
    }

    /**
     * Sets the street
     *
     * @param street the street
     */
    public void setStreet(final String street) {
        this.street = street;
    }

    /**
     * Returns the city
     *
     * @return the city
     */
    public String getCity() {
        return city;
    }

    /**
     * Sets the city
     *
     * @param city the city
     */
    public void setCity(final String city) {
        this.city = city;
    }

    /**
     * Returns the zip
     *
     * @return the zip
     */
    public String getZip() {
        return zip;
    }

    /**
     * Sets the zip
     *
     * @param zip the zip
     */
    public void setZip(final String zip) {
        this.zip = zip;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Address address = (Address) o;

        if (getStreet() != null ? !getStreet().equals(address.getStreet()) : address.getStreet() != null) {
            return false;
        }
        if (getCity() != null ? !getCity().equals(address.getCity()) : address.getCity() != null) {
            return false;
        }
        if (getZip() != null ? !getZip().equals(address.getZip()) : address.getZip() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getStreet() != null ? getStreet().hashCode() : 0;
        result = 31 * result + (getCity() != null ? getCity().hashCode() : 0);
        result = 31 * result + (getZip() != null ? getZip().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Address{"
                + "street='" + street + "'"
                + ", city='" + city + "'"
                + ", zip='" + zip + "'"
                + "}";
    }
}
