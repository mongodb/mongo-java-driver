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

import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.SrvHostValidator;
import com.mongodb.lang.Nullable;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SrvValidationHelper {
    static SrvHostValidator getEffectiveSrvHostValidator(final ClusterSettings settings) {
        return getEffectiveSrvHostValidator(settings.getSrvHostValidator(), settings.getSrvAllowedHostsSuffix(), settings.getSrvHost());
    }
    static SrvHostValidator getEffectiveSrvHostValidator(
            @Nullable final SrvHostValidator srvHostValidator,
            @Nullable final String srvAllowedHostsSuffix,
            @Nullable final String srvHost) {
        if (srvHostValidator != null) {
            return srvHostValidator;
        } else if (srvAllowedHostsSuffix != null) {
            return new DnsSuffixValidator(srvAllowedHostsSuffix);
        } else if (srvHost != null) {
            int firstDot = srvHost.indexOf('.');
            boolean hasThreeParts = firstDot >= 0 && srvHost.indexOf('.', firstDot + 1) >= 0;
            return new DnsSuffixValidator(hasThreeParts ? srvHost.substring(firstDot) : srvHost);
        } else {
            // This should be unreachable
            return h -> false;
        }
    }

    private SrvValidationHelper() {
    }
}
