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

package org.mongodb.codecs;

import org.bson.types.CodeWithScope;
import org.mongodb.DBRef;
import org.mongodb.Encoder;
import org.mongodb.codecs.validators.QueryFieldNameValidator;

import java.util.HashMap;
import java.util.Map;

public class EncoderRegistry {
    //TODO: I think this will go away
    private final Codecs codecs;

    //TODO: this needs to be injected
    private final PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
    //TODO: this needs to be injected
    private final QueryFieldNameValidator defaultValidator = new QueryFieldNameValidator();

    @SuppressWarnings("rawtypes") // going to have some unchecked warnings because of all the casting from Object
    private final Map<Class, Encoder> classToEncoderMap = new HashMap<Class, Encoder>();

    public EncoderRegistry() {
        codecs = new Codecs(primitiveCodecs, defaultValidator, this);
        classToEncoderMap.put(CodeWithScope.class, new CodeWithScopeCodec(codecs));
        classToEncoderMap.put(Iterable.class, new IterableCodec(codecs));
        classToEncoderMap.put(DBRef.class, new DBRefEncoder(codecs));
    }

    public Encoder getDefaultEncoder() {
        return (Encoder) new DocumentCodec(primitiveCodecs, defaultValidator, this);
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) //not cool
    public <T> Encoder<T> get(final Class<T> aClass) {
        for (Map.Entry<Class, Encoder> cur : classToEncoderMap.entrySet()) {
            if (cur.getKey().isAssignableFrom(aClass)) {
                return cur.getValue();
            }
        }
        return null;
    }

    /**
     * This will override any existing encoder associated with the given class
     *
     * @param aClass  the class type that can be encoded by this encoder
     * @param encoder the Encoder that can serialise this class
     * @param <T>     the Type that will be encoded
     */
    public <T> boolean register(final Class<T> aClass, final Encoder<T> encoder) {
        return classToEncoderMap.put(aClass, encoder) != null;
    }

}
