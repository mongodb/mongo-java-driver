/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bson.codecs.configuration.mapper;

import java.util.ArrayList;
import java.util.List;

/**
 * A mapped type for use inside a {@link ClassModelCodec}
 */
@SuppressWarnings("unchecked")
public abstract class MappedType {
    private final Class<?> type;
    private final List<Class<?>> parameterTypes = new ArrayList<Class<?>>();
    private MappedType owner;

    /**
     * Creates a MappedType for the given backing type
     *
     * @param type the Class to map
     */
    public MappedType(final Class<?> type) {
        this.type = type;
    }

    /**
     * Creates a MappedType for the given backing type and the owner
     *
     * @param type  the Class to map
     * @param owner the parent of the MappedType to create
     */
    public MappedType(final Class<?> type, final MappedType owner) {
        this.owner = owner;
        this.type = type;
    }

    /**
     * Adds a parameterized type for a generic type
     *
     * @param parameter type parameter type
     */
    public void addParameter(final Class<?> parameter) {
        parameterTypes.add(parameter);
    }

    /**
     * The owning type of this MappedType
     *
     * @return the owner
     */
    public MappedType getOwner() {
        return owner;
    }

    /**
     * @param <T> the parameterized type of the Class
     * @return the backing class for the MappedType
     */
    public <T> Class<T> getType() {
        return (Class<T>) type;
    }
}
