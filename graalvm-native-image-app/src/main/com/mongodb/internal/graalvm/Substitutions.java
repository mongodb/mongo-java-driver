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
package com.mongodb.internal.graalvm;

import com.mongodb.UnixServerAddress;
import com.mongodb.internal.graalvm.substitution.UnixServerAddressSubstitution;

import static com.mongodb.assertions.Assertions.fail;
import static org.graalvm.nativeimage.ImageInfo.inImageRuntimeCode;

final class Substitutions {
    public static void main(final String... args) {
        assertUnixServerAddressSubstitution();
    }

    private static void assertUnixServerAddressSubstitution() {
        try {
            new UnixServerAddress("/tmp/mongodb-27017.sock");
            if (inImageRuntimeCode()) {
                fail(String.format("%s was not applied", UnixServerAddressSubstitution.class));
            }
        } catch (UnsupportedOperationException e) {
            if (!inImageRuntimeCode()) {
                throw e;
            }
            // expected in GraalVM
        }
    }

    private Substitutions() {
    }
}
