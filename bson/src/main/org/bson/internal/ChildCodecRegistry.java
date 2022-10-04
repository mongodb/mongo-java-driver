/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.internal;


import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

// An implementation of CodecRegistry that is used to detect cyclic dependencies between Codecs
class ChildCodecRegistry<T> implements CodecRegistry {

    private final ChildCodecRegistry<?> parent;
    private final CycleDetectingCodecRegistry registry;
    private final Class<T> codecClass;
    private final List<Type> types;

    ChildCodecRegistry(final CycleDetectingCodecRegistry registry, final Class<T> codecClass) {
        this(registry, codecClass, Collections.emptyList());
    }

    ChildCodecRegistry(final CycleDetectingCodecRegistry registry, final Class<T> codecClass, final List<Type> types) {
        this.codecClass = codecClass;
        this.parent = null;
        this.registry = registry;
        this.types = types;
    }

    private ChildCodecRegistry(final ChildCodecRegistry<?> parent, final Class<T> codecClass, final List<Type> types) {
        this.parent = parent;
        this.codecClass = codecClass;
        this.registry = parent.registry;
        this.types = types;
    }

    public Class<T> getCodecClass() {
        return codecClass;
    }

    public List<Type> getTypes() {
        return types;
    }

    // Gets a Codec, but if it detects a cyclic dependency, return a LazyCodec which breaks the chain.
    public <U> Codec<U> get(final Class<U> clazz) {
        if (hasCycles(clazz)) {
            return new LazyCodec<U>(registry, clazz, types);
        } else {
            return registry.get(new ChildCodecRegistry<>(this, clazz, null));
        }
    }

    @Override
    public <U> Codec<U> get(final Class<U> clazz, final List<Type> typeArguments) {
        if (hasCycles(clazz)) {
            return new LazyCodec<U>(registry, clazz, typeArguments);
        } else {
            return registry.get(new ChildCodecRegistry<>(this, clazz, typeArguments));
        }
    }

    @Override
    public <U> Codec<U> get(final Class<U> clazz, final CodecRegistry registry) {
        return this.registry.get(clazz, registry);
    }

    @SuppressWarnings("rawtypes")
    private <U> Boolean hasCycles(final Class<U> theClass) {
        ChildCodecRegistry current = this;
        while (current != null) {
            if (current.codecClass.equals(theClass)) {
                return true;
            }

            current = current.parent;
        }

        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChildCodecRegistry<?> that = (ChildCodecRegistry) o;

        if (!codecClass.equals(that.codecClass)) {
            return false;
        }
        if (parent != null ? !parent.equals(that.parent) : that.parent != null) {
            return false;
        }
        if (!registry.equals(that.registry)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = parent != null ? parent.hashCode() : 0;
        result = 31 * result + registry.hashCode();
        result = 31 * result + codecClass.hashCode();
        return result;
    }
}
