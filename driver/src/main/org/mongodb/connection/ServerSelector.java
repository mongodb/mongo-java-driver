/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.annotations.ThreadSafe;

/**
 * An interface for selecting a server from a cluster according some preference.
 *
 * Implementations of this interface should ensure that their equals and hashCode methods compare equal preferences as equal,
 * as users of this interface may rely on that behavior to efficiently consolidate handling of multiple requests waiting on a server that
 * can satisfy the preference.
 *
 * @since 3.0.0
 */
@ThreadSafe
public interface ServerSelector {
    ServerDescription choose(ReplicaSetDescription replicaSetDescription);
}
