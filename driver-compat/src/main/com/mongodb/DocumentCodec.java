/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.types.CodeWScope;
import org.mongodb.codecs.Codecs;
import org.mongodb.codecs.EncoderRegistry;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.codecs.validators.QueryFieldNameValidator;

class DocumentCodec extends org.mongodb.codecs.DocumentCodec {
    public DocumentCodec(final PrimitiveCodecs primitiveCodecs) {
        super(new QueryFieldNameValidator(), constructCustomCodecs(primitiveCodecs));
    }

    private static Codecs constructCustomCodecs(final PrimitiveCodecs primitiveCodecs) {
        EncoderRegistry encoderRegistry = new EncoderRegistry();
        Codecs codecs = new Codecs(primitiveCodecs, encoderRegistry);
        encoderRegistry.register(DBRef.class, new DBRefEncoder(codecs));
        encoderRegistry.register(CodeWScope.class, new CodeWScopeCodec(codecs));
        return codecs;
    }
}
