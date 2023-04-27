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
package com.mongodb.internal;

/**
 * This interface exists to work around the unreasonable OpenJDK {@code javac} warning
 * {@code "[try] auto-closeable resource MyAutoCloseable has a member method close() that could throw InterruptedException"}
 * emitted when compiling code like
 * <pre>{@code
 *  class MyAutoCloseable implements AutoCloseable {
 *      ExecutorService executor1 = Executors.newSingleThreadExecutor();
 *      ExecutorService executor2 = Executors.newSingleThreadExecutor();
 *
 *      @Override
 *      @SuppressWarnings("try")
 *      // The method has to declare `throws Exception` because we use the `AutoCloseable` variable type,
 *      // and `AutoCloseable.close` is declared as `throws Exception`.
 *      public void close() throws Exception {
 *          try (AutoCloseable shutdown1 = executor1::shutdownNow;
 *              AutoCloseable shutdown2 = executor2::shutdownNow) {
 *              // we use the `try`-with-resources statement to release multiple resources
 *          }
 *      }
 *  }
 * }</pre>
 * One may avoid the warning by using {@link NoCheckedAutoCloseable} as the type of the {@code shutdown1}, {@code shutdown2} variables
 * and removing the {@code throws Exception} declaration. Alternatively, one may annotate the {@code MyAutoCloseable} class
 * with {@code @SuppressWarnings("try")}, in which case annotating the {@code MyAutoCloseable.close} method
 * with {@code @SuppressWarnings ("try")} is no longer needed, but such an approach is too coarse-grained.
 */
public interface NoCheckedAutoCloseable extends AutoCloseable {
    @Override
    void close();
}
