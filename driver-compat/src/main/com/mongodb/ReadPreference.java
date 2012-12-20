/*
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

package com.mongodb;

// TODO: build this out.
public class ReadPreference {
    org.mongodb.ReadPreference toNew() {
        return org.mongodb.ReadPreference.primary();
    }

    /**
     * @return ReadPreference which reads from primary only
     */
    public static ReadPreference primary() {
        return _PRIMARY;
    }

    /**
     * @return ReadPreference which reads primary if available.
     */
    public static ReadPreference primaryPreferred() {
        return _PRIMARY_PREFERRED;
    }

     /**
     * @return ReadPreference which reads secondary.
     */
    public static ReadPreference secondary() {
        return _SECONDARY;
    }

    /**
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return _SECONDARY_PREFERRED;
    }

    /**
     * @return ReadPreference which reads nearest node.
     */
    public static ReadPreference nearest() {
        return _NEAREST;
    }

    private static final ReadPreference _PRIMARY;
    private static final ReadPreference _SECONDARY;
    private static final ReadPreference _SECONDARY_PREFERRED;
    private static final ReadPreference _PRIMARY_PREFERRED;
    private static final ReadPreference _NEAREST;

    static {
        _PRIMARY = new ReadPreference();
        _SECONDARY = _PRIMARY;
        _SECONDARY_PREFERRED = _PRIMARY;
        _PRIMARY_PREFERRED = _PRIMARY;
        _NEAREST = _PRIMARY;
    }

}
