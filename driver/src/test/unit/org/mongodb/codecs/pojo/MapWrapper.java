package org.mongodb.codecs.pojo;

import java.util.Map;

public class MapWrapper {
    private Map<String, String> theMap;

    public void setTheMap(final Map<String, String> theMap) {
        this.theMap = theMap;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MapWrapper that = (MapWrapper) o;

        if (!theMap.equals(that.theMap)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return theMap.hashCode();
    }

    @Override
    public String toString() {
        return "MapWrapper{theMap=" + theMap + '}';
    }
}
