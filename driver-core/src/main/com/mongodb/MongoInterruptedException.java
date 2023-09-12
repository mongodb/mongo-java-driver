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

package com.mongodb;

import com.mongodb.lang.Nullable;

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.InterruptibleChannel;

/**
 * A driver-specific non-checked counterpart to {@link InterruptedException}.
 * Before this exception is thrown, the {@linkplain Thread#isInterrupted() interrupt status} of the thread will have been set
 * unless the {@linkplain #getCause() cause} is {@link InterruptedIOException}, in which case the driver leaves the status as is.
 * <p>
 * The Java SE API uses exceptions different from {@link InterruptedException} to communicate the same information:</p>
 * <ul>
 *     <li>{@link InterruptibleChannel} uses {@link ClosedByInterruptException}.</li>
 *     <li>{@link Socket#connect(SocketAddress)},
 *     {@linkplain InputStream}/{@link OutputStream} obtained via {@link Socket#getInputStream()}/{@link Socket#getOutputStream()}
 *     use either {@link ClosedByInterruptException} or {@link SocketException}.</li>
 *     <li>There is also {@link InterruptedIOException}, which is documented to an extent as an IO-specific counterpart to
 *     {@link InterruptedException}.</li>
 * </ul>
 * The driver strives to wrap those in {@link MongoInterruptedException} where relevant.
 *
 * @see Thread#interrupt()
 */
public class MongoInterruptedException extends MongoException {
    private static final long serialVersionUID = -4110417867718417860L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     * @param e the cause
     */
    public MongoInterruptedException(@Nullable final String message, @Nullable final Exception e) {
        super(message, e);
    }
}
