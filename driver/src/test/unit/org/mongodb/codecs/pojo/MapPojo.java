package org.mongodb.codecs.pojo;

import java.util.Map;

public class MapPojo {
    private Map<String, Person> map;

    public MapPojo(final Map<String, Person> map) {
        this.map = map;
    }

    public MapPojo() {
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
        return map.equals(((MapPojo) o).map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return "MapPojo{map=" + map + '}';
    }
}
