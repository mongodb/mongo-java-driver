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
package org.mongodb.scala.model.search

import com.mongodb.client.model.search.{ SearchOperatorCombination => JSearchOperatorCombination }

import scala.collection.JavaConverters._

/**
 * Represents a combination of `SearchOperator`s with a rule affecting how they are used when matching documents
 * and calculating the relevance score assigned to each found document.
 * These `SearchOperator`s are called "clauses" in the context of a [[CompoundSearchOperator]].
 *
 * @see [[SearchOperator.compound]]
 * @since 4.7
 */
object SearchOperatorCombination {

  /**
   * Returns a `SearchOperatorCombination` of clauses that must all be satisfied.
   *
   * @param clauses Non-empty clauses.
   * @return The requested `SearchOperatorCombination`.
   */
  def must(clauses: Iterable[_ <: SearchOperator]): MustSearchOperatorCombination =
    JSearchOperatorCombination.must(clauses.asJava)

  /**
   * Returns a `SearchOperatorCombination` of clauses that must all not be satisfied.
   *
   * @param clauses Non-empty clauses.
   * @return The requested `SearchOperatorCombination`.
   */
  def mustNot(clauses: Iterable[_ <: SearchOperator]): MustNotSearchOperatorCombination =
    JSearchOperatorCombination.mustNot(clauses.asJava)

  /**
   * Returns a `SearchOperatorCombination` of clauses that are preferred to be satisfied.
   *
   * @param clauses Non-empty clauses.
   * @return The requested `SearchOperatorCombination`.
   */
  def should(clauses: Iterable[_ <: SearchOperator]): ShouldSearchOperatorCombination =
    JSearchOperatorCombination.should(clauses.asJava)

  /**
   * Returns a `SearchOperatorCombination` of clauses that, similarly to [[must]], must all be satisfied.
   * The difference is that [[filter]] does not affect the relevance score.
   *
   * @param clauses Non-empty clauses.
   * @return The requested `SearchOperatorCombination`.
   */
  def filter(clauses: Iterable[_ <: SearchOperator]): FilterSearchOperatorCombination =
    JSearchOperatorCombination.filter(clauses.asJava)
}
