/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The MongoDriverInformation class allows driver and library authors to add extra information about their library. This information is
 * then available in the MongoD/MongoS logs.
 *
 * <p>
 *     The following metadata can be included when creating a {@code MongoClient}.
 * </p>
 * <ul>
 *     <li>The driver name. Eg: {@code mongo-scala-driver}</li>
 *     <li>The driver version. Eg: {@code 1.2.0}</li>
 *     <li>Extra platform information. Eg: {@code Scala 2.11}</li>
 * </ul>
 * <p>
 *     Note: Library authors are responsible for accepting {@code MongoDriverInformation} from external libraries using their library.
 *     Also all the meta data is limited to 512 bytes and any excess data will be truncated.
 * </p>
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public final class MongoDriverInformation {
    private final List<String> driverNames;
    private final List<String> driverVersions;
    private final List<String> driverPlatforms;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method to create a Builder.
     *
     * @param mongoDriverInformation the mongoDriverInformation to extend
     * @return a builder
     */
    public static Builder builder(final MongoDriverInformation mongoDriverInformation) {
        return new Builder(mongoDriverInformation);
    }

    /**
     * Returns the driverNames
     *
     * @return the driverNames
     */
    public List<String> getDriverNames() {
        return driverNames;
    }

    /**
     * Returns the driverVersions
     *
     * @return the driverVersions
     */
    public List<String> getDriverVersions() {
        return driverVersions;
    }

    /**
     * Returns the driverPlatforms
     *
     * @return the driverPlatforms
     */
    public List<String> getDriverPlatforms() {
        return driverPlatforms;
    }

    /**
     *
     */
    @NotThreadSafe
    public static final class Builder {
        private final MongoDriverInformation driverInformation;
        private String driverName;
        private String driverVersion;
        private String driverPlatform;

        /**
         * Sets the name
         *
         * @param driverName the name
         * @return this
         */
        public Builder driverName(final String driverName) {
            this.driverName = notNull("driverName", driverName);
            return this;
        }

        /**
         * Sets the version
         *
         * <p>
         *     Note: You must also set a driver name if setting a driver version.
         * </p>
         *
         * @param driverVersion the version
         * @return this
         */
        public Builder driverVersion(final String driverVersion) {
            this.driverVersion = notNull("driverVersion", driverVersion);
            return this;
        }

        /**
         * Sets the platform
         *
         * @param driverPlatform the platform
         * @return this
         */
        public Builder driverPlatform(final String driverPlatform) {
            this.driverPlatform = notNull("driverPlatform", driverPlatform);
            return this;
        }

        /**
         * @return the driver information
         */
        public MongoDriverInformation build() {
            isTrue("You must also set the driver name when setting the driver version", !(driverName == null && driverVersion != null));

            List<String> names = prependToList(driverInformation.getDriverNames(), driverName);
            List<String> versions = prependToList(driverInformation.getDriverVersions(), driverVersion);
            List<String> platforms = prependToList(driverInformation.getDriverPlatforms(), driverPlatform);
            return new MongoDriverInformation(names, versions, platforms);
        }

        private List<String> prependToList(final List<String> stringList, final String value) {
            if (value == null) {
                return stringList;
            } else {
                ArrayList<String> newList = new ArrayList<String>();
                newList.add(value);
                newList.addAll(stringList);
                return Collections.unmodifiableList(newList);
            }
        }

        private Builder() {
            List<String> immutableEmptyList = Collections.unmodifiableList(Collections.<String>emptyList());
            driverInformation = new MongoDriverInformation(immutableEmptyList, immutableEmptyList, immutableEmptyList);
        }

        private Builder(final MongoDriverInformation driverInformation) {
            this.driverInformation = notNull("driverInformation", driverInformation);
        }
    }

    private MongoDriverInformation(final List<String> driverNames, final List<String> driverVersions, final List<String> driverPlatforms) {
        this.driverNames = driverNames;
        this.driverVersions = driverVersions;
        this.driverPlatforms = driverPlatforms;
    }
}
