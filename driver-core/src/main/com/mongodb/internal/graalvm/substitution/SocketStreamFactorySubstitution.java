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
package com.mongodb.internal.graalvm.substitution;

import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.internal.connection.SocketStreamFactory;
import com.mongodb.internal.connection.Stream;
import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import static com.mongodb.assertions.Assertions.fail;

@TargetClass(SocketStreamFactory.class)
public final class SocketStreamFactorySubstitution {
    @Substitute
    public Stream create(final ServerAddress serverAddress) {
        if (serverAddress instanceof UnixServerAddress) {
            throw new UnsupportedOperationException("UnixServerAddress is not supported in GraalVM native image");
        }
        return createInternal(serverAddress);
    }

    @Alias
    private Stream createInternal(final ServerAddress serverAddress) {
        throw fail();
    }

    private SocketStreamFactorySubstitution() {
    }
}
