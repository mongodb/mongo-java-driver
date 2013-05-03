package org.mongodb.codecs.pojo;

import java.util.Set;

public class SetPojo {
    private Set<Person> set;

    public SetPojo(final Set<Person> set) {
        this.set = set;
    }

    public SetPojo() {
        //for decoding
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return set.equals(((SetPojo) o).set);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public String toString() {
        return "SetPojo{set=" + set + '}';
    }
}
