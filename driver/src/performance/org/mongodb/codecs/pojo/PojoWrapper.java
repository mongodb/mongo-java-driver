package org.mongodb.codecs.pojo;

public class PojoWrapper {
    private StringWrapper anotherPojo;

    public PojoWrapper(final StringWrapper anotherPojo) {
        this.anotherPojo = anotherPojo;
    }

    public PojoWrapper() {
    }
}
