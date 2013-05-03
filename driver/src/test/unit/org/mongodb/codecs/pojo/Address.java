package org.mongodb.codecs.pojo;

public class Address {
    private String address1 = "Flat 1";
    private String address2 = "Town";

    public String getAddress1() {
        return address1;
    }

    public String getAddress2() {
        return address2;
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

        final Address address = (Address) o;

        return address1.equals(address.address1) && address2.equals(address.address2);
    }

    @Override
    public int hashCode() {
        int result = address1.hashCode();
        result = 31 * result + address2.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Address{"
               + "address1='" + address1 + '\''
               + ", address2='" + address2 + '\''
               + '}';
    }
}