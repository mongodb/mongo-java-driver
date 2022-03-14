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
 * This package defines the Service Provider Interface (SPI) for a DNS provider.  By default the driver will use the
 * JNDI support from com.sun.jndi.dns.DnsContextFactory, but this can be replaced using the JDK's ServerLoader capabilities.
 *
 * @see java.util.ServiceLoader
 */

package com.mongodb.spi.dns;
