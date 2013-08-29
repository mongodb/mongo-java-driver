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

package com.mongodb.codecs;

import com.mongodb.DBRef;
import org.bson.types.CodeWScope;
import org.mongodb.codecs.Codecs;
import org.mongodb.codecs.EncoderRegistry;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.codecs.validators.QueryFieldNameValidator;

public class DocumentCodec extends org.mongodb.codecs.DocumentCodec {
    public DocumentCodec(final PrimitiveCodecs primitiveCodecs) {
        super(new QueryFieldNameValidator(), constructCustomCodecs(primitiveCodecs));
    }

    private static Codecs constructCustomCodecs(final PrimitiveCodecs primitiveCodecs) {
        final EncoderRegistry encoderRegistry = new EncoderRegistry();
        final Codecs codecs = new Codecs(primitiveCodecs, encoderRegistry);
        encoderRegistry.register(DBRef.class, new DBRefEncoder(codecs));
        encoderRegistry.register(CodeWScope.class, new CodeWScopeCodec(codecs));
        return codecs;
    }
}
