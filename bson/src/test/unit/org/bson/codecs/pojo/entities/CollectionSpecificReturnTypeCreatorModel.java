package org.bson.codecs.pojo.entities;

import com.google.common.collect.ImmutableList;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public class CollectionSpecificReturnTypeCreatorModel {
    private final ImmutableList<String> properties;

    @BsonCreator
    public CollectionSpecificReturnTypeCreatorModel(@BsonProperty("properties") List<String> properties) {
        this.properties = ImmutableList.copyOf(properties);
    }

    public ImmutableList<String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollectionSpecificReturnTypeCreatorModel that = (CollectionSpecificReturnTypeCreatorModel) o;

        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        return properties != null ? properties.hashCode() : 0;
    }
}
