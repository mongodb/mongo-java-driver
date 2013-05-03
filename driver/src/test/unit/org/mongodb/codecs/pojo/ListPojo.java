package org.mongodb.codecs.pojo;

import java.util.List;

public class ListPojo {
    private List<Person> list;

    public ListPojo(final List<Person> list) {
        this.list = list;
    }

    public ListPojo() {
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
        return list.equals(((ListPojo) o).list);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    @Override
    public String toString() {
        return "ListPojo{list=" + list + '}';
    }
}
