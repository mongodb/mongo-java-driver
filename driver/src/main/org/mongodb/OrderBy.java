package org.mongodb;

public enum OrderBy {
    ASC(1), DESC(-1);
    private final int intRepresentation;

    OrderBy(final int intRepresentation) {
        this.intRepresentation = intRepresentation;
    }

    public int getIntRepresentation() {
        return intRepresentation;
    }

    public static OrderBy fromInt(final int intRepresentation) {
        switch (intRepresentation) {
            case 1:
                return ASC;
            case -1:
                return DESC;
            default:
                throw new IllegalArgumentException(intRepresentation + " is not a valid index Order");
        }
    }
}
