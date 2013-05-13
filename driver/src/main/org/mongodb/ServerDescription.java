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

package org.mongodb;

import org.mongodb.annotations.Immutable;

@Immutable
public abstract class ServerDescription {

    private final ServerAddress address;
    private final float normalizedPingTime;
    private final boolean ok;
    private final int maxBSONObjectSize;

    protected ServerDescription(final float pingTime, final ServerAddress serverAddress, final int maxBSONObjectSize, final boolean ok,
                                final float latencySmoothFactor, final ServerDescription previous) {
        this.address = serverAddress;
        this.maxBSONObjectSize = maxBSONObjectSize;
        this.ok = ok;
        this.normalizedPingTime = previous == null || !previous.isOk()
                ? pingTime
                : previous.getNormalizedPingTime() + ((pingTime - previous.getNormalizedPingTime()) / latencySmoothFactor);
    }

    public boolean isOk() {
        return ok;
    }

    public int getMaxBSONObjectSize() {
        return maxBSONObjectSize;
    }

    public ServerAddress getServerAddress() {
        return getAddress();
    }

    public ServerAddress getAddress() {
        return address;
    }

    public float getNormalizedPingTime() {
        return normalizedPingTime;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ServerDescription serverDescription = (ServerDescription) o;

        if (getMaxBSONObjectSize() != serverDescription.getMaxBSONObjectSize()) {
            return false;
        }
        if (isOk() != serverDescription.isOk()) {
            return false;
        }
        if (Float.compare(serverDescription.getNormalizedPingTime(), getNormalizedPingTime()) != 0) {
            return false;
        }
        if (!getAddress().equals(serverDescription.getAddress())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getAddress().hashCode();
        result = 31 * result + (getNormalizedPingTime() != +0.0f ? Float.floatToIntBits(getNormalizedPingTime()) : 0);
        result = 31 * result + (isOk() ? 1 : 0);
        result = 31 * result + getMaxBSONObjectSize();
        return result;
    }

    public String toJSON() {
        final StringBuilder buf = new StringBuilder();
        buf.append("{");
        buf.append("address:'").append(getAddress()).append("', ");
        buf.append("ok:").append(isOk()).append(", ");
        buf.append("ping:").append(getNormalizedPingTime()).append(", ");
        buf.append("maxBSONObjectSize:").append(getMaxBSONObjectSize()).append(", ");
        buf.append("}");

        return buf.toString();
    }

    @Override
    public String toString() {
        return "ServerDescription{"
                + "address=" + address
                + ", normalizedPingTime=" + normalizedPingTime
                + ", ok=" + ok
                + ", maxBSONObjectSize="
                + maxBSONObjectSize
                + '}';
    }
}
