package org.mongodb.codecs.pojo;

public class SingleFieldPojo {
    private Object field;

    public SingleFieldPojo(final Object field) {
        this.field = field;
    }

    public SingleFieldPojo() {
    }

    public Object getField() {
        return field;
    }

    //**** All this boilerplate needed just to make testing easy/correct
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return field.equals(((SingleFieldPojo) o).field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }

    @Override
    public String toString() {
        return "SimplePojo{field=" + field + '}';
    }
}
