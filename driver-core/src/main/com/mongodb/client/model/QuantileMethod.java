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
package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This interface represents a quantile method used in quantile accumulators of the {@code $group} and
 * {@code $setWindowFields} stages.
 * <p>
 * It provides methods for creating and converting quantile methods to {@link BsonValue}.
 * </p>
 *
 * @see Accumulators#percentile(String, Object, Object, QuantileMethod)
 * @see Accumulators#median(String, Object, QuantileMethod)
 * @see WindowOutputFields#percentile(String, Object, Object, QuantileMethod, Window)
 * @see WindowOutputFields#median(String, Object, QuantileMethod, Window)
 * @since 4.10
 * @mongodb.server.release 7.0
 */
@Sealed
public interface QuantileMethod {
    /**
     * Returns a {@link QuantileMethod} instance representing the "approximate" quantile method.
     *
     * @return The requested {@link QuantileMethod}.
     */
    static ApproximateQuantileMethod approximate() {
        return new QuantileMethodBson(new BsonString("approximate"));
    }

    /**
     * Creates a {@link QuantileMethod} from a {@link BsonValue} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     * <p>
     * <i>Example</i><br>
     * The following code creates two functionally equivalent {@link QuantileMethod}s,
     * though they may not be {@linkplain Object#equals(Object) equal}.
     * <pre>{@code
     *  QuantileMethod method1 = QuantileMethod.approximate();
     *  QuantileMethod method2 = QuantileMethod.of(new BsonString("approximate"));
     * }</pre>
     * </p>
     *
     * @param method A {@link BsonValue} representing the required {@link QuantileMethod}.
     * @return The requested {@link QuantileMethod}.
     */
    static QuantileMethod of(final BsonValue method) {
        notNull("method", method);
        return new QuantileMethodBson(method);
    }

    /**
     * Converts this object to {@link BsonValue}.
     *
     * @return A {@link BsonValue} representing this {@link QuantileMethod}.
     */
    BsonValue toBsonValue();
}
