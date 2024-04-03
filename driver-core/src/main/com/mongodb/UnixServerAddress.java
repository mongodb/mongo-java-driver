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

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.internal.graalvm.substitution.SocketStreamFactorySubstitution;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents the location of a MongoD unix domain socket.
 * It is {@linkplain SocketStreamFactorySubstitution#create(ServerAddress) not supported in GraalVM native image}.
 *
 * <p>Requires the 'jnr.unixsocket' library.</p>
 * @since 3.7
 */
@Immutable
public final class UnixServerAddress extends ServerAddress {
    private static final long serialVersionUID = 154466643544866543L;

    /**
     * Creates a new instance
     * @param path the path of the MongoD unix domain socket.
     */
    public UnixServerAddress(final String path) {
        super(notNull("The path cannot be null", path));
        isTrueArgument("The path must end in .sock", path.endsWith(".sock"));
    }

    @Override
    public String toString() {
        return getHost();
    }
}
