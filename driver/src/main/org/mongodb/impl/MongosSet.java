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

package org.mongodb.impl;

import java.util.Collections;
import java.util.List;

// TODO: Should this be public and move out of impl?
class MongosSet {

    private final MongosSetMemberDescription best;
    private final List<MongosSetMemberDescription> members;

    public MongosSet(final List<MongosSetMemberDescription> members, final MongosSet previous) {
        this.members = Collections.unmodifiableList(members);
        best = calculatePreferred(previous);
    }

    private MongosSetMemberDescription calculatePreferred(final MongosSet previous) {
        MongosSetMemberDescription retVal = null;
        MongosSetMemberDescription preferredFromPrevious = previous == null ? null : previous.getPreferred();
        for (MongosSetMemberDescription cur : members) {
            if (cur.isOk()) {
                if (preferredFromPrevious != null && preferredFromPrevious.getServerAddress().equals(cur.getServerAddress())) {
                    retVal = cur;
                    break;
                }
                if (retVal == null || cur.getNormalizedPingTime() < retVal.getNormalizedPingTime()) {
                    retVal = cur;
                }
            }
        }
        return retVal;
    }

    public MongosSetMemberDescription getPreferred() {
        return best;
    }

    public List<MongosSetMemberDescription> getAll() {
        return members;
    }
}
