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

package com.mongodb.internal.connection.tlschannel;

import java.io.IOException;
import java.nio.channels.ByteChannel;

/**
 * Base class for exceptions used to control flow.
 *
 * <p>Because exceptions of this class are not used to signal errors, they don't contain stack
 * traces, to improve efficiency.
 *
 * <p>This class inherits from {@link IOException} as a compromise to allow {@link TlsChannel} to
 * throw it while still implementing the {@link ByteChannel} interface.
 */
public abstract class TlsChannelFlowControlException extends IOException {
  private static final long serialVersionUID = -2394919487958591959L;

  public TlsChannelFlowControlException() {
  }

  /** For efficiency, override this method to do nothing. */
  @Override
  public Throwable fillInStackTrace() {
    return this;
  }
}
