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

package org.bson.internal;

import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public final class CodecRegistryHelper {

    public static CodecRegistry createRegistry(final CodecRegistry codecRegistry, final UuidRepresentation uuidRepresentation) {
        CodecRegistry retVal = codecRegistry;
        if (uuidRepresentation != UuidRepresentation.JAVA_LEGACY) {
            if (codecRegistry instanceof CodecProvider) {
                retVal = new OverridableUuidRepresentationCodecRegistry((CodecProvider) codecRegistry, uuidRepresentation);
            } else {
                throw new CodecConfigurationException("Changing the default UuidRepresentation requires a CodecRegistry that also "
                        +    "implements the CodecProvider interface");
            }
        }
        return retVal;
    }

    private CodecRegistryHelper() {
    }
}
