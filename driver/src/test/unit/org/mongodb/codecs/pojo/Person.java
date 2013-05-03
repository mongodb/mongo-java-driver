package org.mongodb.codecs.pojo;

//CHECKSTYLE:OFF
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
