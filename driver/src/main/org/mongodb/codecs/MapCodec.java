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

import org.bson.BSONWriter;
import org.mongodb.codecs.validators.Validator;

import java.util.Map;

public class MapCodec implements ComplexTypeEncoder<Map<String, ?>> {
    private final Codecs codecs;
    private final Validator<String> validator;

    public MapCodec(final Codecs codecs, final Validator<String> validator) {
        this.codecs = codecs;
        this.validator = validator;
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Map<String, ?> value) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, ?> entry : value.entrySet()) {
            String fieldName = entry.getKey();
            validateFieldName(fieldName);

            bsonWriter.writeName(fieldName);
            codecs.encode(bsonWriter, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    public Class<Map> getEncoderClass() {
        return Map.class;
    }

    private void validateFieldName(final String fieldName) {
        validator.validate(fieldName);
    }
}
