package org.mongodb.codecs;

import java.util.HashSet;
import java.util.Set;

public class HashSetFactory implements CollectionFactory {
    @Override
    public <E> Set<E> createCollection() {
        return new HashSet<E>();
    }
}
