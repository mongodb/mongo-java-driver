package org.mongodb.codecs.pojo;

public class ArrayWrapper {
    //set via reflection in encoder/decoder tests
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    private int[] ints;

    public ArrayWrapper(final int[] ints) {
        this.ints = ints;
    }

    //required for decoding
    @SuppressWarnings("UnusedDeclaration")
    public ArrayWrapper() {
    }
}
