/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 */

package com.mongodb;

/**
 * A non-checked exception indicating that the driver has been interrupted by a call to Thread.interrupt.
 *
 * @see Thread#interrupt()
 * @see InterruptedException
 */
public class MongoInterruptedException extends MongoException {
    private static final long serialVersionUID = -4110417867718417860L;

    public MongoInterruptedException(final InterruptedException e) {
        super("A driver operation has been interrupted", e);
    }

    public MongoInterruptedException(final String message, final InterruptedException e) {
        super(message, e);
    }
}
