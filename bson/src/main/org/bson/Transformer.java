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

package org.bson;

/**
 * Transforms objects that can be converted to BSON into other Java types, and vice versa.
 * @deprecated there is no direct replacement for this class, but you can achieve the same effect more flexibly using a
 *  {@link org.bson.codecs.configuration.CodecRegistry}.
 */
@Deprecated
public interface Transformer {
    /**
     * Turns the {@code objectToTransform} into some other {@code Object}. This can either be turning a simple BSON-friendly object into a
     * different Java type, or it can be turning a Java type that can't automatically be converted into BSON into something that can.
     *
     * @param objectToTransform the object that needs to be transformed.
     * @return the new transformed object.
     */
    Object transform(Object objectToTransform);
}
