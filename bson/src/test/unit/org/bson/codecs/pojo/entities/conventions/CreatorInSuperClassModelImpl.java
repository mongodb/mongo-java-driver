package org.bson.codecs.pojo.entities.conventions;

public class CreatorInSuperClassModelImpl extends CreatorInSuperClassModel {
    private final String propertyA;
    private final String propertyB;

    CreatorInSuperClassModelImpl(String propertyA, String propertyB) {
        this.propertyA = propertyA;
        this.propertyB = propertyB;
    }

    @Override
    public String getPropertyA() {
        return propertyA;
    }

    @Override
    public String getPropertyB() {
        return propertyB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreatorInSuperClassModelImpl that = (CreatorInSuperClassModelImpl) o;

        if (propertyA != null ? !propertyA.equals(that.propertyA) : that.propertyA != null) return false;
        return propertyB != null ? propertyB.equals(that.propertyB) : that.propertyB == null;
    }

    @Override
    public int hashCode() {
        int result = propertyA != null ? propertyA.hashCode() : 0;
        result = 31 * result + (propertyB != null ? propertyB.hashCode() : 0);
        return result;
    }
}
