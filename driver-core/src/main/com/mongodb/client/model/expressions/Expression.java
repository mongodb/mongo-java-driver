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

package com.mongodb.client.model.expressions;

import com.mongodb.annotations.Evolving;

/**
 * Expressions express values that may be represented in, or computations that
 * may be performed within, a MongoDB server. Each expression resolves to some
 * value, much like any Java expression ultimately resolves to some value, and
 * expressions may be thought of as boxed values, except that resolution might
 * potentially happen on a server, rather than locally.
 *
 * <p>Users should treat these interfaces as sealed, and must not implement any
 * expression interfaces.
 *
 * <p>Expressions are typed. It is possible to execute expressions against data
 * that is of the wrong type, such as by applying the "not" boolean expression
 * to a document field that is an integer. This API does not define the output
 * in such cases. Where data might have divergent types (for example, if there
 * is some field that could be null, or missing, or a boolean, or an int) then
 * users should make it explicit what should be done in each case. Likewise,
 * unless otherwise specified, this API does not define the order of evaluation
 * for all arguments, as well as whether all arguments to some expression shall
 * be evaluated.
 *
 * @see Expressions
 */
@Evolving
public interface Expression {

}
