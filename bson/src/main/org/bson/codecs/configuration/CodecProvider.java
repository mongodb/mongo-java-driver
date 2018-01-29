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

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

/**
 * A provider of {@code Codec} instances.  Typically, an instance of a class implementing this interface would be used to construct a
 * {@code CodecRegistry}.
 *
 * <p>While the {@code CodecProvider} interface adds no stipulations to the general contract for the Object.equals,
 * programmers who implement the {@code CodecProvider} interface "directly" must exercise care if they choose to override the
 * {@code Object.equals}. It is not necessary to do so, and the simplest course of action is to rely on Object's implementation, but the
 * implementer may wish to implement a "value comparison" in place of the default "reference comparison."</p>
 *
 * @since 3.0
 */
public interface CodecProvider {

    /**
     * Get a {@code Codec} using the given context, which includes, most importantly, the Class for which a {@code Codec} is required.
     *
     * @param clazz the Class for which to get a Codec
     * @param registry the registry to use for resolving dependent Codec instances
     * @param <T> the type of the class for which a Codec is required
     * @return the Codec instance, which may be null, if this source is unable to provide one for the requested Class
     */
    <T> Codec<T> get(Class<T> clazz, CodecRegistry registry);
}
