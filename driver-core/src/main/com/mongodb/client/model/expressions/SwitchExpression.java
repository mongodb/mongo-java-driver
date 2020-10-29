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

package com.mongodb.client.model.expressions;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;

/**
 * Evaluates a series of case expressions. When it finds an expression which evaluates to true, $switch executes a specified expression
 * and breaks out of the control flow.
 *
 * @since 4.?
 */
@Immutable
public final class SwitchExpression implements Expression {
    private final List<Branch> branches;
    @Nullable
    private final Expression defaultExpr;

    SwitchExpression(final List<Branch> branches, @Nullable final Expression defaultExpr) {
        this.branches = notNull("branches", branches);
        this.defaultExpr = defaultExpr;
    }

    /**
     * The path to take if no branch case expression evaluates to true.
     *
     * @param defaultExpr an expression to evaluate if no branch evaluates to true
     * @return a new SwitchExpression
     */
    public SwitchExpression defaultExpr(final Expression defaultExpr) {
        return new SwitchExpression(branches, defaultExpr);
    }

    /**
     * An array of control branch documents.
     *
     * @return a list of branches, which may not be null.
     */
    public List<Branch> getBranches() {
        return unmodifiableList(branches);
    }

    /**
     * The path to take if no branch evaluates to true.
     *
     * @return an expression to evaluate if no branch evaluates to true. This may be null
     */
    @Nullable
    public Expression getDefault() {
        return defaultExpr;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        BsonDocument switchDocument = new BsonDocument("branches",
                branches.stream().map(branch ->
                        new BsonDocument("case", branch.getCase().toBsonValue(codecRegistry))
                                .append("then", branch.getThen().toBsonValue(codecRegistry)))
                        .collect(Collectors.toCollection(BsonArray::new)));
        if (defaultExpr != null) {
            switchDocument.append("default", defaultExpr.toBsonValue(codecRegistry));
        }
        return new BsonDocument("$switch", switchDocument);
    }

    @Override
    public String toString() {
        return "SwitchExpression{" +
                "branches=" + branches +
                ", defaultExpr=" + defaultExpr +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SwitchExpression that = (SwitchExpression) o;
        return Objects.equals(branches, that.branches) &&
                Objects.equals(defaultExpr, that.defaultExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(branches, defaultExpr);
    }
}
