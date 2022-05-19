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
package com.mongodb.client.model.search;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Evolving;

/**
 * @see SearchScore#boost(FieldSearchPath)
 * @since 4.7
 */
@Evolving
@Beta(Beta.Reason.CLIENT)
public interface PathBoostSearchScore extends SearchScore {
    /**
     * Creates a new {@link PathBoostSearchScore} with the value to fall back to
     * if the field specified via {@link SearchScore#boost(FieldSearchPath)} is not found in a document.
     *
     * @param fallback The fallback value. Unlike {@link SearchScore#constant(float)}, does not have constraints.
     * @return A new {@link PathBoostSearchScore}.
     */
    PathBoostSearchScore undefined(float fallback);
}
