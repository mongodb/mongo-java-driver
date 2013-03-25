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

import org.mongodb.ServerAddress;

// TODO: Should this be public and move out of impl?  Should it have a common base class with ReplicaSetMember
class MongosSetMember {
    private final ServerAddress serverAddress;
    private final float pingTime;
    private final boolean ok;
    private final int maxBSONObjectSize;

    public MongosSetMember(final ServerAddress serverAddress, final float pingTime, final boolean ok, final int maxBSONObjectSize) {
        this.serverAddress = serverAddress;
        this.pingTime = pingTime;
        this.ok = ok;
        this.maxBSONObjectSize = maxBSONObjectSize;
    }

    public MongosSetMember(final ServerAddress serverAddress) {
        this(serverAddress, 0, false, 0);
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public float getPingTime() {
        return pingTime;
    }

    public boolean isOk() {
        return ok;
    }

    public int getMaxBSONObjectSize() {
        return maxBSONObjectSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongosSetMember that = (MongosSetMember) o;

        if (maxBSONObjectSize != that.maxBSONObjectSize) return false;
        if (Float.compare(that.pingTime, pingTime) != 0) return false;
        if (ok != that.ok) return false;
        if (!serverAddress.equals(that.serverAddress)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = serverAddress.hashCode();
        result = 31 * result + (pingTime != +0.0f ? Float.floatToIntBits(pingTime) : 0);
        result = 31 * result + (ok ? 1 : 0);
        result = 31 * result + maxBSONObjectSize;
        return result;
    }
}
