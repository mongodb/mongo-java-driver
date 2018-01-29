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

package com.mongodb.connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * Holds an array of three integers representing the server version, e.g. [3, 4, 1].
 *
 * @since 3.0
 */
public class ServerVersion implements Comparable<ServerVersion> {
    private final List<Integer> versionList;

    /**
     * Creates a server version which will compare as less than all other valid versions
     */
    public ServerVersion() {
        this.versionList = Collections.unmodifiableList(Arrays.asList(0, 0, 0));
    }

    /**
     * Constructs a new instance with the given version list of integers.
     *
     * @param versionList a non-null, three-item list of integers
     */
    public ServerVersion(final List<Integer> versionList) {
        notNull("versionList", versionList);
        isTrue("version array has three elements", versionList.size() == 3);
        this.versionList = Collections.unmodifiableList(new ArrayList<Integer>(versionList));
    }

    /**
     * Constructs a new instance with the given major and minor versions and a patch version of 0.
     *
     * @param majorVersion the major version
     * @param minorVersion the minor version
     */
    public ServerVersion(final int majorVersion, final int minorVersion) {
        this(asList(majorVersion, minorVersion, 0));
    }

    /**
     * Gets the version list.
     *
     * @return an unmodifiable list of three integers
     */
    public List<Integer> getVersionList() {
        return versionList;
    }

    @Override
    public int compareTo(final ServerVersion o) {
        int retVal = 0;
        for (int i = 0; i < versionList.size(); i++) {
            retVal = versionList.get(i).compareTo(o.versionList.get(i));
            if (retVal != 0) {
                break;
            }
        }
        return retVal;

    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerVersion that = (ServerVersion) o;

        if (!versionList.equals(that.versionList)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return versionList.hashCode();
    }

    @Override
    public String toString() {
        return "ServerVersion{"
               + "versionList=" + versionList
               + '}';
    }
}
