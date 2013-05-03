package org.mongodb.codecs.pojo;

public class Name {
    private String firstName = "Eric";
    private String surname = "Smith";

    //**** Really simple POJO needs all this boilerplate just to make testing easy/correct
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Name name = (Name) o;

        return firstName.equals(name.firstName) && surname.equals(name.surname);
    }

    @Override
    public int hashCode() {
        int result = firstName.hashCode();
        result = 31 * result + surname.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Name{"
               + "firstName='" + firstName + '\''
               + ", surname='" + surname + '\''
               + '}';
    }

    public String getFirstName() {
        return firstName;
    }

    public String getSurname() {
        return surname;
    }
}
