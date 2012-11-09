/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.rs;

import org.bson.util.annotations.Immutable;
import org.mongodb.ServerAddress;

@Immutable
public class Node {

    Node(float pingTime, ServerAddress addr, int maxBsonObjectSize, boolean ok) {
        this._pingTime = pingTime;
        this._addr = addr;
        this._maxBsonObjectSize = maxBsonObjectSize;
        this._ok = ok;
    }

    public boolean isOk() {
        return _ok;
    }

    public int getMaxBsonObjectSize() {
        return _maxBsonObjectSize;
    }

    public ServerAddress getServerAddress() {
        return _addr;
    }

    protected final ServerAddress _addr;
    protected final float _pingTime;
    protected final boolean _ok;
    protected final int _maxBsonObjectSize;

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Node node = (Node) o;

        if (_maxBsonObjectSize != node._maxBsonObjectSize) return false;
        if (_ok != node._ok) return false;
        if (Float.compare(node._pingTime, _pingTime) != 0) return false;
        if (!_addr.equals(node._addr)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _addr.hashCode();
        result = 31 * result + (_pingTime != +0.0f ? Float.floatToIntBits(_pingTime) : 0);
        result = 31 * result + (_ok ? 1 : 0);
        result = 31 * result + _maxBsonObjectSize;
        return result;
    }

    public String toJSON() {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        buf.append("address:'").append(_addr).append("', ");
        buf.append("ok:").append(_ok).append(", ");
        buf.append("ping:").append(_pingTime).append(", ");
        buf.append("maxBsonObjectSize:").append(_maxBsonObjectSize).append(", ");
        buf.append("}");

        return buf.toString();
    }
}
