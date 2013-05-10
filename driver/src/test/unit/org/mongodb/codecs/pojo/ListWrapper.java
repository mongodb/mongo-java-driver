package org.mongodb.codecs.pojo;

import java.util.List;

public class ListWrapper {
    private List<Integer> integerList;

    public ListWrapper(final List<Integer> integerList) {
        this.integerList = integerList;
    }

    public ListWrapper() {
    }

    //**** Boilerplate
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ListWrapper that = (ListWrapper) o;

        if (!integerList.equals(that.integerList)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return integerList.hashCode();
    }

    @Override
    public String toString() {
        return "ListWrapper{integerList=" + integerList + '}';
    }
}
