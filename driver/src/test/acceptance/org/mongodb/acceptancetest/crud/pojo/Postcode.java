package org.mongodb.acceptancetest.crud.pojo;

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
