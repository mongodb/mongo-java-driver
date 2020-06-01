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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngineResult;

public class Util {

  private static final Logger logger = LoggerFactory.getLogger(Util.class);

  public static void assertTrue(boolean condition) {
    if (!condition) throw new AssertionError();
  }

  /**
   * Convert a {@link SSLEngineResult} into a {@link String}, this is needed because the supplied
   * method includes a log-breaking newline.
   *
   * @param result the SSLEngineResult
   * @return the resulting string
   */
  public static String resultToString(SSLEngineResult result) {
    return String.format(
        "status=%s,handshakeStatus=%s,bytesConsumed=%d,bytesConsumed=%d",
        result.getStatus(),
        result.getHandshakeStatus(),
        result.bytesProduced(),
        result.bytesConsumed());
  }

  public static int getJavaMajorVersion() {
    String version = System.getProperty("java.version");
    if (version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if (dot != -1) {
        version = version.substring(0, dot);
      }
    }
    return Integer.parseInt(version);
  }
}
