package org.bson.codecs.pojo.entities;

public class InterfaceUpperBoundsModelAbstractImpl extends InterfaceUpperBoundsModelAbstract {
    private String name;
    private InterfaceModelImpl nestedModel;

    public InterfaceUpperBoundsModelAbstractImpl() {
    }

    public InterfaceUpperBoundsModelAbstractImpl(String name, InterfaceModelImpl nestedModel) {
        this.name = name;
        this.nestedModel = nestedModel;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public InterfaceModelImpl getNestedModel() {
        return nestedModel;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNestedModel(InterfaceModelImpl nestedModel) {
        this.nestedModel = nestedModel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InterfaceUpperBoundsModelAbstractImpl that = (InterfaceUpperBoundsModelAbstractImpl) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return nestedModel != null ? nestedModel.equals(that.nestedModel) : that.nestedModel == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (nestedModel != null ? nestedModel.hashCode() : 0);
        return result;
    }
}
