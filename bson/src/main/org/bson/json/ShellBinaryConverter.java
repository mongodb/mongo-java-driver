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

package org.bson.json;

import org.bson.BsonBinary;

import java.util.Base64;

import static java.lang.String.format;

class ShellBinaryConverter implements Converter<BsonBinary> {
    @Override
    public void convert(final BsonBinary value, final StrictJsonWriter writer) {
        writer.writeRaw(format("new BinData(%s, \"%s\")", value.getType() & 0xFF,
                Base64.getEncoder().encodeToString(value.getData())));
    }
}
