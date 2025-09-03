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

import com.mongodb.UnixServerAddress;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(UnixServerAddress.class)
public final class UnixServerAddressSubstitution {
    @Substitute
    private static void checkNotInGraalVmNativeImage() {
        throw new UnsupportedOperationException("UnixServerAddress is not supported in GraalVM native image");
    }

    private UnixServerAddressSubstitution() {
    }
}
