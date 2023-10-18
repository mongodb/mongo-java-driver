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

package com.mongodb.internal;

import com.mongodb.MongoCommandException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Exceptions {
    public static final class MongoCommandExceptions {
        public static int extractErrorCode(final BsonDocument response) {
            return extractErrorCodeAsBson(response).intValue();
        }

        public static String extractErrorCodeName(final BsonDocument response) {
            return extractErrorCodeNameAsBson(response).getValue();
        }

        public static BsonArray extractErrorLabelsAsBson(final BsonDocument response) {
            return response.getArray("errorLabels", new BsonArray());
        }

        /**
         * Constructs a {@link MongoCommandException} with the data from the {@code original} redacted for security purposes.
         */
        public static MongoCommandException redacted(final MongoCommandException original) {
            BsonDocument originalResponse = original.getResponse();
            BsonDocument redactedResponse = new BsonDocument();
            for (SecurityInsensitiveResponseField field : SecurityInsensitiveResponseField.values()) {
                redactedResponse.append(field.fieldName(), field.fieldValue(originalResponse));
            }
            MongoCommandException result = new MongoCommandException(redactedResponse, original.getServerAddress());
            result.setStackTrace(original.getStackTrace());
            return result;
        }

        private static BsonNumber extractErrorCodeAsBson(final BsonDocument response) {
            return response.getNumber("code", new BsonInt32(-1));
        }

        private static BsonString extractErrorCodeNameAsBson(final BsonDocument response) {
            return response.getString("codeName", new BsonString(""));
        }

        @VisibleForTesting(otherwise = PRIVATE)
        public enum SecurityInsensitiveResponseField {
            CODE("code", MongoCommandExceptions::extractErrorCodeAsBson),
            CODE_NAME("codeName", MongoCommandExceptions::extractErrorCodeNameAsBson),
            ERROR_LABELS("errorLabels", MongoCommandExceptions::extractErrorLabelsAsBson);

            private final String fieldName;
            private final Function<BsonDocument, BsonValue> fieldValueExtractor;

            SecurityInsensitiveResponseField(final String fieldName, final Function<BsonDocument, BsonValue> fieldValueExtractor) {
                this.fieldName = fieldName;
                this.fieldValueExtractor = fieldValueExtractor;
            }

            String fieldName() {
                return fieldName;
            }

            BsonValue fieldValue(final BsonDocument response) {
                return fieldValueExtractor.apply(response);
            }

            @VisibleForTesting(otherwise = PRIVATE)
            public static Set<String> fieldNames() {
                return Stream.of(SecurityInsensitiveResponseField.values())
                        .map(SecurityInsensitiveResponseField::fieldName)
                        .collect(Collectors.toSet());
            }
        }

        private MongoCommandExceptions() {
        }
    }

    private Exceptions() {
    }
}
