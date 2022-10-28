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
 *
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.util;

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Access to NIO sun.misc.Cleaner, allowing caller to deterministically deallocate a given
 * sun.nio.ch.DirectBuffer.
 */
public class DirectBufferDeallocator {

  private static final Logger LOGGER = Loggers.getLogger("connection.tls");

  private interface Deallocator {
    void free(ByteBuffer bb);
  }

  private static class Java8Deallocator implements Deallocator {

    /*
     * Getting instance of cleaner from buffer (sun.misc.Cleaner)
     */

    final Method cleanerAccessor;
    final Method clean;

    Java8Deallocator() {
      try {
        cleanerAccessor = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
      } catch (NoSuchMethodException | ClassNotFoundException t) {
        throw new RuntimeException(t);
      }
    }

    @Override
    public void free(ByteBuffer bb) {
      try {
        clean.invoke(cleanerAccessor.invoke(bb));
      } catch (IllegalAccessException | InvocationTargetException t) {
        throw new RuntimeException(t);
      }
    }
  }

  private static class Java9Deallocator implements Deallocator {

    /*
     * Clean is of type jdk.internal.ref.Cleaner, but this type is not accessible, as it is not exported by default.
     * Using workaround through sun.misc.Unsafe.
     */

    final Object unsafe;
    final Method invokeCleaner;

    Java9Deallocator() {
      try {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        // avoiding getUnsafe methods, as it is explicitly filtered out from reflection API
        Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        unsafe = theUnsafe.get(null);
        invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
      } catch (NoSuchMethodException
          | ClassNotFoundException
          | IllegalAccessException
          | NoSuchFieldException t) {
        throw new RuntimeException(t);
      }
    }

    @Override
    public void free(ByteBuffer bb) {
      try {
        invokeCleaner.invoke(unsafe, bb);
      } catch (IllegalAccessException | InvocationTargetException t) {
        throw new RuntimeException(t);
      }
    }
  }

  private final Deallocator deallocator;

  public DirectBufferDeallocator() {
    if (Util.getJavaMajorVersion() >= 9) {
      deallocator = new Java9Deallocator();
      LOGGER.debug("initialized direct buffer deallocator for java >= 9");
    } else {
      deallocator = new Java8Deallocator();
      LOGGER.debug("initialized direct buffer deallocator for java < 9");
    }
  }

  public void deallocate(ByteBuffer buffer) {
    deallocator.free(buffer);
  }
}
