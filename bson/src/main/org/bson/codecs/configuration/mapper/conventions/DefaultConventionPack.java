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

package org.bson.codecs.configuration.mapper.conventions;

/**
 * Provides a default set of conventions to use with the driver.
 */
public class DefaultConventionPack extends ConventionPack {
    /**
     * Creates the pack
     */
    public DefaultConventionPack() {
        addConvention(new LowerCaseClassNamingConvention());
        addConvention(new BeanPropertiesConvention());
    }

    @Override
    public boolean isMappable(final Class<?> clazz) {
        return true;
    }
}
