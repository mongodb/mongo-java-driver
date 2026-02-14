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
package com.mongodb.internal.async.function;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

@NotThreadSafe
final class CallbackChain {
    private int enteringCounter;

    CallbackChain() {
        enteringCounter = 0;
    }

    static boolean execute(@Nullable final CallbackChain chain, @Nullable final Element element) {
        if (element == null) {
            return false;
        }
        if (chain != null) {
            return chain.execute(element);
        } else {
            element.execute();
            return true;
        }
    }

    private boolean execute(final Element element) {
        boolean reentered = ++enteringCounter > 1;
        try {
            if (reentered) {
                return false;
            }
            Element next = element.execute();
            while (next != null) {
                next = next.execute();
            }
        } finally {
            enteringCounter--;
        }
        return true;
    }

    interface Element {
        @Nullable
        Element execute();
    }
}
