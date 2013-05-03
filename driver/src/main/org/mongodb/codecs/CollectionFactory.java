package org.mongodb.codecs;

import java.util.Collection;

public interface CollectionFactory {
    <E> Collection<E> createCollection();
}
