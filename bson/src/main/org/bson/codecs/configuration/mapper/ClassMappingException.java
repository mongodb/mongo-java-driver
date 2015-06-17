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

/**
 * Indicates an error when trying to map a class.
 */
public class ClassMappingException extends RuntimeException {
    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * Indicates an error when trying to map a class.
     *
     * @param message the reason for the error
     */
    public ClassMappingException(final String message) {
        super(message);
    }
}
