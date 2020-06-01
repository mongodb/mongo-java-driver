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

/**
 * TLS Channel is a library that implements a ByteChannel interface to a TLS (Transport Layer
 * Security) connection. The library delegates all cryptographic operations to the standard Java TLS
 * implementation: SSLEngine; effectively hiding it behind an easy-to-use streaming API, that allows
 * to securitize JVM applications with minimal added complexity.
 *
 * <p>In other words, a simple library that allows the programmer to have TLS using the same
 * standard socket API used for plaintext, just like OpenSSL does for C, only for Java, filling a
 * specially painful missing feature of the standard Java library.
 */
package com.mongodb.internal.connection.tlschannel;
