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
 * Represents some standard weight values to help break up the spectrum of possible weight values.
 */
public final class Weights {
    public static final int DEFAULT = 0;

    public static final int BUILT_IN_CONVENTION = 100;

    public static final int USER_CONVENTION = 200;

    public static final int BUILT_IN_ATTRIBUTE = 300;

    public static final int USER_ATTRIBUTE = 400;

    public static final int USER_CODE = 500;

    private Weights() {
    }
}
