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

import org.graalvm.nativeimage.ImageInfo;

import static com.mongodb.assertions.Assertions.fail;

public final class Substitutions {
    /**
     * @throws AssertionError If called at native image run time,
     * unless the method body is substituted using the GraalVM native image technology.
     * If not called at native image run time, then does not throw.
     * @see SubstitutionsSubstitution#assertUsed()
     */
    public static void assertUsed() throws AssertionError {
        if (ImageInfo.inImageRuntimeCode()) {
            fail("The body of this method must be substituted when compiling using the GraalVM native image technology");
        }
    }

    private Substitutions() {
    }
}
