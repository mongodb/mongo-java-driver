package org.mongodb.performance.codecs.pojo;

public class TwoPojoWrapper {
    private Object value1;
    private Object value2;

    public TwoPojoWrapper(final Object value1, final Object value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public TwoPojoWrapper() {
    }
}
