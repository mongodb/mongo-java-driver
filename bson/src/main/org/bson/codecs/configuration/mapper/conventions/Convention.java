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

import org.bson.codecs.configuration.mapper.ClassModel;

/**
 * Defines a convention to be applied when mapping a class.
 */
public interface Convention {

    /**
     * This method applies this Convention to the ClassModel given
     *
     * @param model the ClassModel to process
     */
    void apply(ClassModel model);

    /**
     * The phase in which this Convention gets applied
     *
     * @return the phase
     */
    String getPhase();

    /**
     * @return the weight to be applied to any suggested values coming from this Convention
     * @see org.bson.codecs.configuration.mapper.WeightedValue
     */
    Integer getWeight();
}
