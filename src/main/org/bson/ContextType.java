/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson;

/// <summary>
/// Used by BsonReaders and BsonWriters to represent the current context.
/// </summary>
public enum ContextType
{
    /// <summary>
    /// The top level of a BSON document.
    /// </summary>
    TOP_LEVEL,
    /// <summary>
    /// A (possibly embedded) BSON document.
    /// </summary>
    DOCUMENT,
    /// <summary>
    /// A BSON array.
    /// </summary>
    ARRAY,
    /// <summary>
    /// A JAVASCRIPT_WITH_SCOPE BSON value.
    /// </summary>
    JAVASCRIPT_WITH_SCOPE,
    /// <summary>
    /// The scope document of a JAVASCRIPT_WITH_SCOPE BSON value.
    /// </summary>
    SCOPE_DOCUMENT
}