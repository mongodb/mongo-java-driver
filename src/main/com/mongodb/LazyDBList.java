/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.bson.LazyBSONCallback;
import org.bson.io.BSONByteBuffer;

public class LazyDBList extends org.bson.LazyDBList {

    public LazyDBList(final byte[] data, final LazyBSONCallback callback) {
        super(data, callback);
    }

    public LazyDBList(final byte[] data, final int offset, final LazyBSONCallback callback) {
        super(data, offset, callback);
    }

    public LazyDBList(final BSONByteBuffer buffer, final LazyBSONCallback callback) {
        super(buffer, callback);
    }

    public LazyDBList(final BSONByteBuffer buffer, final int offset, final LazyBSONCallback callback) {
        super(buffer, offset, callback);
    }
}
