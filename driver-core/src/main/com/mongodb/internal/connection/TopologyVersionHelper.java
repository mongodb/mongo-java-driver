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
package com.mongodb.internal.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.TopologyVersion;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.Comparator;
import java.util.Optional;

final class TopologyVersionHelper {
    /**
     * Defines a <a href="https://mathworld.wolfram.com/StrictOrder.html">strict</a> (irreflexive)
     * <a href="https://mathworld.wolfram.com/PartialOrder.html">partial order</a> (vacuously antisymmetric, transitive)
     * on a set of all {@link TopologyVersion}s, including {@code null}.
     * Since this <a href="https://mathworld.wolfram.com/BinaryRelation.html">binary relation</a>
     * is not a <a href="https://mathworld.wolfram.com/TotallyOrderedSet.html">total order</a>,
     * it cannot be expressed as a {@link Comparator}.
     * <p>
     * There are two reasons we need this strict comparison in addition to the
     * {@linkplain #newerOrEqual(TopologyVersion, TopologyVersion) non-strict one}:
     * <ul>
     *     <li>A candidate {@link ServerDescription} has information besides
     *     {@linkplain ServerDescription#getTopologyVersion() topology version}, and that information must be applied by the client even if
     *     the topology version has not changed.</li>
     *     <li>The client may {@linkplain ConnectionPool#invalidate() pause} a {@link ConnectionPool}
     *     and then {@linkplain ConnectionPool#ready() mark it ready} based on receiving a new {@link ServerDescription}
     *     from a {@link ServerMonitor}, without the server for that pool changing its topology version.
     *     Consequently, a candidate {@link ServerDescription} cannot be rejected solely based on the fact that its
     *     {@linkplain ServerDescription#getTopologyVersion() topology version} is equal to the one that the client considers current.</li>
     * </ul>
     *
     * @return {@code true} if and only if {@code current} is considered newer than {@code candidate}.
     * @see #newerOrEqual(TopologyVersion, TopologyVersion)
     */
    static boolean newer(@Nullable final TopologyVersion current, @Nullable final TopologyVersion candidate) {
        return compare(current, candidate) > 0;
    }

    /**
     * Defines a <a href="https://en.wikipedia.org/wiki/Reflexive_relation#Quasi-reflexivity">quasi-reflexive</a>,
     * antisymmetric, transitive <a href="https://mathworld.wolfram.com/BinaryRelation.html">binary relation</a>
     * (neither a <a href="https://mathworld.wolfram.com/PartialOrder.html">partial order</a>
     * nor a <a href="https://mathworld.wolfram.com/StrictOrder.html">strict</a>
     * <a href="https://mathworld.wolfram.com/PartialOrder.html">partial order</a>
     * because it is neither reflexive nor irreflexive)
     * on a set of all {@link TopologyVersion}s, including {@code null}.
     * Since this binary relation is not a <a href="https://mathworld.wolfram.com/TotallyOrderedSet.html">total order</a>,
     * it cannot be expressed as a {@link Comparator}.
     *
     * @return {@code true} if and only if {@code current} is considered newer than or equal to {@code candidate}.
     * @see #newer(TopologyVersion, TopologyVersion)
     * @see #topologyVersion(Throwable)
     */
    static boolean newerOrEqual(@Nullable final TopologyVersion current, @Nullable final TopologyVersion candidate) {
        return compare(current, candidate) >= 0;
    }

    /**
     * @return A {@link TopologyVersion} that must be applied by the client unless it has already learned
     * {@linkplain #newerOrEqual(TopologyVersion, TopologyVersion) it or a newer one}.
     */
    static Optional<TopologyVersion> topologyVersion(@Nullable final Throwable t) {
        TopologyVersion result = null;
        if (t instanceof MongoCommandException) {
            BsonDocument rawTopologyVersion = ((MongoCommandException) t).getResponse()
                    .getDocument("topologyVersion", null);
            if (rawTopologyVersion != null) {
                result = new TopologyVersion(rawTopologyVersion);
            }
        }
        return Optional.ofNullable(result);
    }

    private static int compare(@Nullable final TopologyVersion o1, @Nullable final TopologyVersion o2) {
        if (o1 == null || o2 == null) {
            return -1;
        }
        if (o1.getProcessId().equals(o2.getProcessId())) {
            return Long.compare(o1.getCounter(), o2.getCounter());
        } else {
            return -1;
        }
    }

    private TopologyVersionHelper() {
        throw new AssertionError();
    }
}
