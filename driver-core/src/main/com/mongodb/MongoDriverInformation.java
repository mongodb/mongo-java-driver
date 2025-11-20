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

package com.mongodb;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.Sealed;
import com.mongodb.internal.client.DriverInformation;
import com.mongodb.internal.connection.ConcreteMongoDriverInformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
@Sealed
public abstract class MongoDriverInformation {


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
        return new Builder((ConcreteMongoDriverInformation) mongoDriverInformation);
    }

    /**
     * Returns the driverNames
     *
     * @return the driverNames
     */
    public abstract List<String> getDriverNames();

    /**
     * Returns the driverVersions
     *
     * @return the driverVersions
     */
    public abstract List<String> getDriverVersions();

    /**
     * Returns the driverPlatforms
     *
     * @return the driverPlatforms
     */
    public abstract List<String> getDriverPlatforms();

    /**
     *
     */
    @NotThreadSafe
    public static final class Builder {
        private final ConcreteMongoDriverInformation mongoDriverInformation;
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
            DriverInformation driverInformation = new DriverInformation(driverName, driverVersion, driverPlatform);
            if (mongoDriverInformation.getDriverInformationList().contains(driverInformation)) {
                return mongoDriverInformation;
            }

            List<DriverInformation> driverInformationList = new ArrayList<>(mongoDriverInformation.getDriverInformationList());
            driverInformationList.add(driverInformation);
            return new ConcreteMongoDriverInformation(Collections.unmodifiableList(driverInformationList));
        }

        private Builder() {
            mongoDriverInformation = new ConcreteMongoDriverInformation(Collections.emptyList());
        }

        private Builder(final ConcreteMongoDriverInformation driverInformation) {
            this.mongoDriverInformation = notNull("driverInformation", driverInformation);
        }
    }
}
