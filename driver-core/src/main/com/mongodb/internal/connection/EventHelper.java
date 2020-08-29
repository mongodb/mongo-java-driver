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

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import java.util.Objects;

final class EventHelper {

    /**
     * Determine whether the two cluster descriptions are effectively equivalent for the purpose of cluster event
     * generation, according to the equality rules enumerated in the Server Discovery and Monitoring specification.
     */
    static boolean wouldDescriptionsGenerateEquivalentEvents(final ClusterDescription current,
                                                             final ClusterDescription previous) {
        if (!exceptionsEquals(current.getSrvResolutionException(), previous.getSrvResolutionException())) {
            return false;
        }
        if (current.getServerDescriptions().size() != previous.getServerDescriptions().size()) {
            return false;
        }
        for (ServerDescription curNew: current.getServerDescriptions()) {
            ServerDescription matchingPrev = null;
            for (ServerDescription curPrev: previous.getServerDescriptions()) {
                if (curNew.getAddress().equals(curPrev.getAddress())) {
                    matchingPrev = curPrev;
                    break;
                }
            }
            if (!wouldDescriptionsGenerateEquivalentEvents(curNew, matchingPrev)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determine whether the two server descriptions are effectively equivalent for the purpose of server event
     * generation, according to the equality rules enumerated in the Server Discovery and Monitoring specification.
     */
    static boolean wouldDescriptionsGenerateEquivalentEvents(final ServerDescription current,
                                                             final ServerDescription previous) {
        if (current == previous) {
            return true;
        }
        if (previous == null || current == null) {
            return false;
        }
        if (current.isOk() != previous.isOk()) {
            return false;
        }
        if (current.getState() != previous.getState()) {
            return false;
        }
        if (current.getType() != previous.getType()) {
            return false;
        }
        if (current.getMinWireVersion() != previous.getMinWireVersion()) {
            return false;
        }
        if (current.getMaxWireVersion() != previous.getMaxWireVersion()) {
            return false;
        }
        if (!Objects.equals(current.getCanonicalAddress(), previous.getCanonicalAddress())) {
            return false;
        }
        if (!current.getHosts().equals(previous.getHosts())) {
            return false;
        }
        if (!current.getPassives().equals(previous.getPassives())) {
            return false;
        }
        if (!current.getArbiters().equals(previous.getArbiters())) {
            return false;
        }
        if (!current.getTagSet().equals(previous.getTagSet())) {
            return false;
        }
        if (!Objects.equals(current.getSetName(), previous.getSetName())) {
            return false;
        }
        if (!Objects.equals(current.getSetVersion(), previous.getSetVersion())) {
            return false;
        }
        if (!Objects.equals(current.getElectionId(), previous.getElectionId())) {
            return false;
        }
        if (!Objects.equals(current.getPrimary(), previous.getPrimary())) {
            return false;
        }
        if (!Objects.equals(current.getLogicalSessionTimeoutMinutes(), previous.getLogicalSessionTimeoutMinutes())) {
            return false;
        }
        if (!Objects.equals(current.getTopologyVersion(), previous.getTopologyVersion())) {
            return false;
        }
        if (!exceptionsEquals(current.getException(), previous.getException())) {
            return false;
        }
        return true;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean exceptionsEquals(final Throwable current, final Throwable previous) {
        if (current == null || previous == null) {
            return current == previous;
        }
        // Compare class equality and message as exceptions rarely override equals
        if (!Objects.equals(current.getClass(), previous.getClass())) {
            return false;
        }

        if (!Objects.equals(current.getMessage(), previous.getMessage())) {
            return false;
        }
        return true;
    }


    private EventHelper() {}
}
