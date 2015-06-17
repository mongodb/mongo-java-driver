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

import com.fasterxml.classmate.members.ResolvedMethod;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * Represents the metamodel of a method on a class mapped by a ClassModel
 */
public class MethodModel {
    private final ClassModel owner;
    private final CodecRegistry registry;
    private final ResolvedMethod method;

    /**
     * @param model The ClassModel on which this method can be found
     * @param registry the registry to use when looking for Codecs
     * @param method the method being modeled
     */
    public MethodModel(final ClassModel model, final CodecRegistry registry, final ResolvedMethod method) {
        owner = model;
        this.registry = registry;
        this.method = method;
    }


    /**
     * @return the number of arguments this method takes
     */
    public int getArgumentCount() {
        return method.getArgumentCount();
    }

    /**
     * @param position which argument to handle
     * @return the type of the argument in the requested position
     */
    public Class<?> getArgumentType(final int position) {
        return method.getArgumentType(position).getErasedType();
    }

    /**
     * @return the name of the method
     */
    public String getName() {
        return method.getName();
    }

    /**
     * @return the return type of the method
     */
    public Class<?> getReturnType() {
        return method.getReturnType().getErasedType();
    }
}
