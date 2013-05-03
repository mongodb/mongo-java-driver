package org.mongodb.codecs;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayListFactory implements CollectionFactory {
    @Override
    public Collection createCollection() {
        return new ArrayList();
    }
}
