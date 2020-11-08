/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import java.util.ArrayDeque;
import java.util.Deque;

final class AssertionContext {

    private final Deque<ContextElement> contextStack = new ArrayDeque<>();

    public void push(final ContextElement contextElement) {
        contextStack.push(contextElement);
    }

    public void pop() {
        contextStack.pop();
    }

    public String getMessage(final String rootMessage) {
        StringBuilder builder = new StringBuilder();

        builder.append(rootMessage).append("\n\n");
        builder.append("Assertion Context:\n\n");

        for (ContextElement contextElement : contextStack) {
            builder.append(contextElement.toString()).append('\n');
        }

        return builder.toString();
    }
}
