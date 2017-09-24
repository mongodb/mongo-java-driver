package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public abstract class CreatorInSuperClassModel {
    @BsonCreator
    public static CreatorInSuperClassModel newInstance(@BsonProperty("propertyA") String propertyA,
                                                       @BsonProperty("propertyB") String propertyB) {
        return new CreatorInSuperClassModelImpl(propertyA, propertyB);
    }
    public abstract String getPropertyA();
    public abstract String getPropertyB();
}
