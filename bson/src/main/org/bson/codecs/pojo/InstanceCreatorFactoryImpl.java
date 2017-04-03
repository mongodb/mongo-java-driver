/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;

import java.lang.reflect.Constructor;

import static java.lang.String.format;

final class InstanceCreatorFactoryImpl<T> implements InstanceCreatorFactory<T> {
    private final String className;
    private final Constructor<T> constructor;

    InstanceCreatorFactoryImpl(final String className, final Constructor<T> constructor) {
        this.className = className;
        this.constructor = constructor;
    }

    @Override
    public InstanceCreator<T> create() {
        if (constructor == null) {
            throw new CodecConfigurationException(format("Cannot find a no-arg constructor for '%s'. Either create one or "
                    + "provide your own InstanceCreatorFactory.", className));
        }
        return new InstanceCreatorInstanceImpl<T>(constructor);
    }
}
