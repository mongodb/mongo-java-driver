package org.mongodb.acceptancetest.crud.pojo;

public class Address {
    private String addressLineOne;
    private String town;
    private Postcode postcode;

    //CHECKSTYLE:OFF
    //necessary evil until we do smart stuff around identifying the ID
    private String _id;
    //CHECKSTYLE:ON

    public Address(final String addressLineOne, final String town, final Postcode postcode) {
        this.addressLineOne = addressLineOne;
        this.town = town;
        this.postcode = postcode;
    }

    public Address() {
        // no-args constructor required for decoding from database
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

        Address address = (Address) o;

        return !(_id != null ? !_id.equals(address._id) : address._id != null)
               && addressLineOne.equals(address.addressLineOne)
               && postcode.equals(address.postcode)
               && town.equals(address.town);
    }

    @Override
    public int hashCode() {
        int result = addressLineOne.hashCode();
        result = 31 * result + town.hashCode();
        result = 31 * result + postcode.hashCode();
        result = 31 * result + (_id != null ? _id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Address{"
               + "addressLineOne='" + addressLineOne + '\''
               + ", town='" + town + '\''
               + ", postcode=" + postcode
               + ", _id='" + _id + '\''
               + '}';
    }
}
