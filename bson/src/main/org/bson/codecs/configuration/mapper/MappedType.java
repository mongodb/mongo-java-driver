package org.bson.codecs.configuration.mapper;

import java.util.ArrayList;
import java.util.List;

public class MappedType {
    private MappedType owner;
    private Class type;
    private final List<Class> parameterTypes = new ArrayList<Class>();

    public MappedType(final Class type) {
        this.type = type;
    }

    public MappedType(final Class type, final MappedType owner) {
        this.owner = owner;
        this.type = type;
    }

    public Class getType() {
        return type;
    }

    public MappedType getOwner() {
        return owner;
    }

    public void addParameter(Class parameter) {
        parameterTypes.add(parameter);
    }
}
