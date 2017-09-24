package org.bson.codecs.pojo.entities;

import com.google.common.collect.ImmutableList;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public class CollectionSpecificReturnTypeModel {
    private ImmutableList<String> properties;

    public CollectionSpecificReturnTypeModel() {
    }

    public CollectionSpecificReturnTypeModel(List<String> properties) {
        this.properties = ImmutableList.copyOf(properties);
    }

    public ImmutableList<String> getProperties() {
        return properties;
    }

    public void setProperties(List<String> properties) {
        this.properties = ImmutableList.copyOf(properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollectionSpecificReturnTypeModel that = (CollectionSpecificReturnTypeModel) o;

        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        return properties != null ? properties.hashCode() : 0;
    }
}
