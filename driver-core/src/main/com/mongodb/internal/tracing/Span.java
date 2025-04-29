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

package com.mongodb.internal.tracing;

public interface Span {
    Span EMPTY = new Span() {
        @Override
        public void tag(final String key, final String value) {
        }

        @Override
        public void event(final String event) {
        }

        @Override
        public void error(final Throwable throwable) {
        }

        @Override
        public void end() {
        }

        @Override
        public TraceContext context() {
            return TraceContext.EMPTY;
        }
    };

    void tag(String key, String value);

    void event(String event);

    void error(Throwable throwable);

    void end();

    TraceContext context();
}
