/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.ServerAddress;
import com.mongodb.TagSet;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import org.bson.types.ObjectId;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.connection.ServerType.UNKNOWN;

/**
 * Immutable snapshot state of a server.
 *
 * @since 3.0
 */
@Immutable
public class ServerDescription {

    static final int MIN_DRIVER_WIRE_VERSION = 1;
    static final int MAX_DRIVER_WIRE_VERSION = 6;

    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB

    private final ServerAddress address;

    private final ServerType type;
    private final String canonicalAddress;
    private final Set<String> hosts;
    private final Set<String> passives;
    private final Set<String> arbiters;
    private final String primary;
    private final int maxDocumentSize;
    private final TagSet tagSet;
    private final String setName;
    private final long roundTripTimeNanos;
    private final boolean ok;
    private final ServerConnectionState state;
    private final ServerVersion version;

    private final int minWireVersion;
    private final int maxWireVersion;

    private final ObjectId electionId;
    private final Integer setVersion;
    private final Date lastWriteDate;
    private final long lastUpdateTimeNanos;

    private final Integer logicalSessionTimeoutMinutes;

    private final Throwable exception;

    /**
     * Gets a Builder for creating a new ServerDescription instance.
     *
     * @return a new Builder for ServerDescription.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the string representing the host name and port that this member of a replica set was configured with,
     * e.g. {@code "somehost:27019"}. This is typically derived from the "me" field from the "isMaster" command response.
     *
     * @return the host name and port that this replica set member is configured with.
     */
    public String getCanonicalAddress() {
        return canonicalAddress;
    }

    /**
     * Gets the session timeout in minutes.
     *
     * @return the session timeout in minutes, or null if sessions are not supported by this server
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    public Integer getLogicalSessionTimeoutMinutes() {
        return logicalSessionTimeoutMinutes;
    }

    /**
     * A builder for creating ServerDescription.
     */
    @NotThreadSafe
    public static class Builder {
        private ServerAddress address;
        private ServerType type = UNKNOWN;
        private String canonicalAddress;
        private Set<String> hosts = Collections.emptySet();
        private Set<String> passives = Collections.emptySet();
        private Set<String> arbiters = Collections.emptySet();
        private String primary;
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private TagSet tagSet = new TagSet();
        private String setName;
        private long roundTripTimeNanos;
        private boolean ok;
        private ServerConnectionState state;
        private ServerVersion version = new ServerVersion();
        private int minWireVersion = 0;
        private int maxWireVersion = 0;
        private ObjectId electionId;
        private Integer setVersion;
        private Date lastWriteDate;
        private long lastUpdateTimeNanos = Time.nanoTime();
        private Integer logicalSessionTimeoutMinutes;

        private Throwable exception;

        /**
         * Sets the address of the server.
         *
         * @param address the address of the server
         * @return this
         */
        public Builder address(final ServerAddress address) {
            this.address = address;
            return this;
        }

        /**
         * Sets the canonical host name and port of this server. This is typically derived from the "me" field contained in the "isMaster"
         * command. response.
         *
         * @param canonicalAddress the host name and port as a string
         *
         * @return this
         */
        public Builder canonicalAddress(final String canonicalAddress) {
            this.canonicalAddress = canonicalAddress;
            return this;
        }

        /**
         * Sets the type of the server, for example whether it's a standalone or in a replica set.
         *
         * @param type the Server type
         * @return this
         */
        public Builder type(final ServerType type) {
            this.type = notNull("type", type);
            return this;
        }

        /**
         * Sets all members of the replica set that are neither hidden, passive, nor arbiters.
         *
         * @param hosts A Set of strings in the format of "[hostname]:[port]" that contains all members of the replica set that are neither
         *              hidden, passive, nor arbiters.
         * @return this
         */
        public Builder hosts(final Set<String> hosts) {
            this.hosts = hosts == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(hosts));
            return this;
        }

        /**
         * Sets the passive members of the replica set.
         *
         * @param passives A Set of strings in the format of "[hostname]:[port]" listing all members of the replica set which have a
         *                 priority of 0.
         * @return this
         */
        public Builder passives(final Set<String> passives) {
            this.passives = passives == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(passives));
            return this;
        }

        /**
         * Sets the arbiters in the replica set
         *
         * @param arbiters A Set of strings in the format of "[hostname]:[port]" containing all members of the replica set that are
         *                 arbiters.
         * @return this
         */
        public Builder arbiters(final Set<String> arbiters) {
            this.arbiters = arbiters == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(arbiters));
            return this;
        }

        /**
         * Sets the address of the current primary in the replica set
         *
         * @param primary A string in the format of "[hostname]:[port]" listing the current primary member of the replica set.
         * @return this
         */
        public Builder primary(final String primary) {
            this.primary = primary;
            return this;
        }

        /**
         * The maximum permitted size of a BSON object in bytes for this mongod process. Defaults to 16MB.
         *
         * @param maxDocumentSize the maximum size a document can be
         * @return this
         */
        public Builder maxDocumentSize(final int maxDocumentSize) {
            this.maxDocumentSize = maxDocumentSize;
            return this;
        }

        /**
         * A set of any tags assigned to this member.
         *
         * @param tagSet a TagSet with all the tags for this server.
         * @return this
         */
        public Builder tagSet(final TagSet tagSet) {
            this.tagSet = tagSet == null ? new TagSet() : tagSet;
            return this;
        }

        /**
         * Set the time it took to make the round trip for requesting this information from the server
         *
         * @param roundTripTime the time taken
         * @param timeUnit      the units of the time taken
         * @return this
         */
        public Builder roundTripTime(final long roundTripTime, final TimeUnit timeUnit) {
            this.roundTripTimeNanos = timeUnit.toNanos(roundTripTime);
            return this;
        }

        /**
         * Sets the name of the replica set
         *
         * @param setName the name of the replica set
         * @return this
         */
        public Builder setName(final String setName) {
            this.setName = setName;
            return this;
        }

        /**
         * The isOK() result from requesting this information from MongoDB
         *
         * @param ok true if the request executed correctly
         * @return this
         */
        public Builder ok(final boolean ok) {
            this.ok = ok;
            return this;
        }

        /**
         * The current state of the connection to the server.
         *
         * @param state ServerConnectionState representing whether the server has been successfully connected to
         * @return this
         */
        public Builder state(final ServerConnectionState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the server version
         *
         * @param version a ServerVersion representing which version of MongoDB is running on this server
         * @return this
         */
        public Builder version(final ServerVersion version) {
            notNull("version", version);
            this.version = version;
            return this;
        }

        /**
         * The earliest version of the wire protocol that this MongoDB server is capable of using to communicate with clients.
         *
         * @param minWireVersion the minimum protocol version supported by this server
         * @return this
         * @mongodb.server.release 2.6
         */
        public Builder minWireVersion(final int minWireVersion) {
            this.minWireVersion = minWireVersion;
            return this;
        }

        /**
         * The latest version of the wire protocol that this MongoDB server is capable of using to communicate with clients.
         *
         * @param maxWireVersion the maximum protocol version supported by this server
         * @return this
         * @mongodb.server.release 2.6
         */
        public Builder maxWireVersion(final int maxWireVersion) {
            this.maxWireVersion = maxWireVersion;
            return this;
        }

        /**
         * Sets the electionId reported by this server.
         *
         * @param electionId the electionId
         * @return this
         */
        public Builder electionId(final ObjectId electionId) {
            this.electionId = electionId;
            return this;
        }

        /**
         * Sets the setVersion reported by this server.
         *
         * @param setVersion the set version
         * @return this
         */
        public Builder setVersion(final Integer setVersion) {
            this.setVersion = setVersion;
            return this;
        }

        /**
         * Sets the lastWriteDate reported by this server
         *
         * @param lastWriteDate the last write date, which may be null for servers prior to 3.4
         * @return this
         *
         * @since 3.4
         * @mongodb.server.release 3.4
         */
        public Builder lastWriteDate(final Date lastWriteDate) {
            this.lastWriteDate = lastWriteDate;
            return this;
        }

        /**
         * Sets the last update time for this description, which is simply the time that the server description was created.
         * A monotonic clock such as {@link System#nanoTime()} should be used to initialize this value.
         *
         * @param lastUpdateTimeNanos the last update time of this server description
         * @return this
         *
         * @since 3.4
         */
        public Builder lastUpdateTimeNanos(final long lastUpdateTimeNanos) {
            this.lastUpdateTimeNanos = lastUpdateTimeNanos;
            return this;
        }

        /**
         * Sets the session timeout in minutes.
         *
         * @param logicalSessionTimeoutMinutes the session timeout in minutes, or null if sessions are not supported by this server
         * @return this
         * @mongodb.server.release 3.6
         * @since 3.6
         */
        public Builder logicalSessionTimeoutMinutes(final Integer logicalSessionTimeoutMinutes) {
            this.logicalSessionTimeoutMinutes = logicalSessionTimeoutMinutes;
            return this;
        }


        /**
         * Sets the exception thrown while attempting to determine the server description.
         *
         * @param exception the exception
         * @return this
         */
        public Builder exception(final Throwable exception) {
            this.exception = exception;
            return this;
        }

        /**
         * Create a new ServerDescription from the settings in this builder.
         *
         * @return a new server description
         */
        public ServerDescription build() {
            return new ServerDescription(this);
        }
    }

    /**
     * Return whether the server is compatible with the driver. An incompatible server is one that has a min wire version greater that the
     * driver's max wire version or a max wire version less than the driver's min wire version.
     *
     * @return true if the server is compatible with the driver.
     */
    public boolean isCompatibleWithDriver() {
        if (!ok) {
            return true;
        }

        if (minWireVersion > MAX_DRIVER_WIRE_VERSION) {
            return false;
        }

        if (maxWireVersion < MIN_DRIVER_WIRE_VERSION) {
            return false;
        }

        return true;
    }

    /**
     * Get the default maximum document size.
     *
     * @return the default maximum document size
     */
    public static int getDefaultMaxDocumentSize() {
        return DEFAULT_MAX_DOCUMENT_SIZE;
    }

    /**
     * Get the default minimum wire version
     *
     * @return the default minimum wire version
     */
    public static int getDefaultMinWireVersion() {
        return 0;
    }

    /**
     * Get the default maximum wire version
     *
     * @return the default maximum wire version
     */
    public static int getDefaultMaxWireVersion() {
        return 0;
    }

    /**
     * Gets the address of this server
     *
     * @return a ServerAddress containing the details of the address of this server.
     */
    public ServerAddress getAddress() {
        return address;
    }

    /**
     * Gets whether this server is a replica set member.
     *
     * @return true if this server is part of a replica set
     */
    public boolean isReplicaSetMember() {
        return type.getClusterType() == ClusterType.REPLICA_SET;
    }

    /**
     * Gets whether this is a server that is the entry point to a sharded instance of MongoDB.
     *
     * @return true if this server is a mongos instance
     */
    public boolean isShardRouter() {
        return type == SHARD_ROUTER;
    }

    /**
     * Gets whether this is part of a replica set/sharded system, or is a single server.
     *
     * @return true if this is a single server
     */
    public boolean isStandAlone() {
        return type == STANDALONE;
    }

    /**
     * Returns whether this can be treated as a primary server.
     *
     * @return true if this server is the primary in a replica set, is a mongos, or is a single standalone server
     */
    public boolean isPrimary() {
        return ok && (type == REPLICA_SET_PRIMARY || type == SHARD_ROUTER || type == STANDALONE);
    }

    /**
     * Returns whether this can be treated as a secondary server.
     *
     * @return true if this server is a secondary in a replica set, is a mongos, or is a single standalone server
     */
    public boolean isSecondary() {
        return ok && (type == REPLICA_SET_SECONDARY || type == SHARD_ROUTER || type == STANDALONE);
    }

    /**
     * Get a Set of strings in the format of "[hostname]:[port]" that contains all members of the replica set that are neither hidden,
     * passive, nor arbiters.
     *
     * @return all members of the replica set that are neither hidden, passive, nor arbiters.
     */
    public Set<String> getHosts() {
        return hosts;
    }

    /**
     * Gets the passive members of the replica set.
     *
     * @return A set of strings in the format of "[hostname]:[port]" listing all members of the replica set which have a priority of 0.
     */
    public Set<String> getPassives() {
        return passives;
    }

    /**
     * Gets the arbiters in the replica set
     *
     * @return A Set of strings in the format of "[hostname]:[port]" containing all members of the replica set that are arbiters.
     */
    public Set<String> getArbiters() {
        return arbiters;
    }

    /**
     * Gets the address of the current primary in the replica set
     *
     * @return A string in the format of "[hostname]:[port]" listing the current primary member of the replica set.
     */
    public String getPrimary() {
        return primary;
    }

    /**
     * The maximum permitted size of a BSON object in bytes for this mongod process. Defaults to 16MB.
     *
     * @return the maximum size a document can be
     */
    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    /**
     * A set of all tags assigned to this member.
     *
     * @return a TagSet with all the tags for this server.
     */
    public TagSet getTagSet() {
        return tagSet;
    }

    /**
     * The earliest version of the wire protocol that this MongoDB server is capable of using to communicate with clients.
     *
     * @return the minimum protocol version supported by this server
     * @mongodb.server.release 2.6
     */
    public int getMinWireVersion() {
        return minWireVersion;
    }

    /**
     * The latest version of the wire protocol that this MongoDB server is capable of using to communicate with clients.
     *
     * @return the maximum protocol version supported by this server
     * @mongodb.server.release 2.6
     */
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    /**
     * The replica set electionid reported by this MongoDB server.
     *
     * @return the electionId, which may be null
     */
    public ObjectId getElectionId() {
        return electionId;
    }

    /**
     * The replica set setVersion reported by this MongoDB server.
     *
     * @return the setVersion, which may be null
     */
    public Integer getSetVersion() {
        return setVersion;
    }

    /**
     * Gets the last write date.
     * @return the last write date, which may be null
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Date getLastWriteDate() {
        return lastWriteDate;
    }

    /**
     * Gets the time that this server description was created, using a monotonic clock like {@link System#nanoTime()}.
     *
     * @param timeUnit the time unit
     * @return the last update time in the given unit
     *
     * @since 3.4
     */
    public long getLastUpdateTime(final TimeUnit timeUnit) {
        return timeUnit.convert(lastUpdateTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns true if the server has the given tags.  A server of either type {@code ServerType.STANDALONE} or {@code
     * ServerType.SHARD_ROUTER} is considered to have all tags, so this method will always return true for instances of either of those
     * types.
     *
     * @param desiredTags the tags
     * @return true if this server has the given tags
     */
    public boolean hasTags(final TagSet desiredTags) {
        if (!ok) {
            return false;
        }

        if (type == STANDALONE || type == SHARD_ROUTER) {
            return true;
        }

        return tagSet.containsAll(desiredTags);
    }

    /**
     * Gets the name of the replica set
     *
     * @return the name of the replica set
     */
    public String getSetName() {
        return setName;
    }

    /**
     * The isOK() result from requesting this information from the server
     *
     * @return true if the request executed correctly
     */
    public boolean isOk() {
        return ok;
    }

    /**
     * Gets the current state of the connection to the server.
     *
     * @return ServerConnectionState representing whether the server has been successfully connected to
     */
    public ServerConnectionState getState() {
        return state;
    }

    /**
     * Gets the type of the server, for example whether it's a standalone or in a replica set.
     *
     * @return the server type
     */
    public ServerType getType() {
        return type;
    }

    /**
     * Gets the type of the cluster this server is in (for example, replica set).
     *
     * @return a ClusterType representing the type of the cluster this server is in
     */
    public ClusterType getClusterType() {
        return type.getClusterType();
    }

    /**
     * Gets the server version
     *
     * @return a ServerVersion representing which version of MongoDB is running on this server
     */
    public ServerVersion getVersion() {
        return version;
    }

    /**
     * Get the time it took to make the round trip for requesting this information from the server in nanoseconds.
     *
     * @return the time taken to request the information, in nano seconds
     */
    public long getRoundTripTimeNanos() {
        return roundTripTimeNanos;
    }

    /**
     * Gets the exception thrown while attempting to determine the server description.  This is useful for diagnostic purposed when
     * determining the root cause of a connectivity failure.
     *
     * @return the exception, which may be null
     */
    public Throwable getException() {
        return exception;
    }

    /**
     * Returns true if this instance is equals to @code{o}.  Note that equality is defined to NOT include the round trip time.
     *
     * @param o the object to compare to
     * @return true if this instance is equals to @code{o}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerDescription that = (ServerDescription) o;

        if (maxDocumentSize != that.maxDocumentSize) {
            return false;
        }
        if (ok != that.ok) {
            return false;
        }
        if (!address.equals(that.address)) {
            return false;
        }
        if (!arbiters.equals(that.arbiters)) {
            return false;
        }
        if (canonicalAddress != null ? !canonicalAddress.equals(that.canonicalAddress) : that.canonicalAddress != null) {
            return false;
        }
        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (!passives.equals(that.passives)) {
            return false;
        }
        if (primary != null ? !primary.equals(that.primary) : that.primary != null) {
            return false;
        }
        if (setName != null ? !setName.equals(that.setName) : that.setName != null) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (!tagSet.equals(that.tagSet)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (!version.equals(that.version)) {
            return false;
        }
        if (minWireVersion != that.minWireVersion) {
            return false;
        }
        if (maxWireVersion != that.maxWireVersion) {
            return false;
        }
        if (electionId != null ? !electionId.equals(that.electionId) : that.electionId != null) {
            return false;
        }
        if (setVersion != null ? !setVersion.equals(that.setVersion) : that.setVersion != null) {
            return false;
        }
        if (lastWriteDate != null ? !lastWriteDate.equals(that.lastWriteDate) : that.lastWriteDate != null) {
            return false;
        }

        if (lastUpdateTimeNanos != that.lastUpdateTimeNanos) {
            return false;
        }

        if (logicalSessionTimeoutMinutes != null
                    ? !logicalSessionTimeoutMinutes.equals(that.logicalSessionTimeoutMinutes)
                    : that.logicalSessionTimeoutMinutes != null) {
            return false;
        }

        // Compare class equality and message as exceptions rarely override equals
        Class<?> thisExceptionClass = exception != null ? exception.getClass() : null;
        Class<?> thatExceptionClass = that.exception != null ? that.exception.getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return false;
        }

        String thisExceptionMessage = exception != null ? exception.getMessage() : null;
        String thatExceptionMessage = that.exception != null ? that.exception.getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (canonicalAddress != null ? canonicalAddress.hashCode() : 0);
        result = 31 * result + hosts.hashCode();
        result = 31 * result + passives.hashCode();
        result = 31 * result + arbiters.hashCode();
        result = 31 * result + (primary != null ? primary.hashCode() : 0);
        result = 31 * result + maxDocumentSize;
        result = 31 * result + tagSet.hashCode();
        result = 31 * result + (setName != null ? setName.hashCode() : 0);
        result = 31 * result + (electionId != null ? electionId.hashCode() : 0);
        result = 31 * result + (setVersion != null ? setVersion.hashCode() : 0);
        result = 31 * result + (lastWriteDate != null ? lastWriteDate.hashCode() : 0);
        result = 31 * result + (int) (lastUpdateTimeNanos ^ (lastUpdateTimeNanos >>> 32));
        result = 31 * result + (ok ? 1 : 0);
        result = 31 * result + state.hashCode();
        result = 31 * result + version.hashCode();
        result = 31 * result + minWireVersion;
        result = 31 * result + maxWireVersion;
        result = 31 * result + (logicalSessionTimeoutMinutes != null ? logicalSessionTimeoutMinutes.hashCode() : 0);
        result = 31 * result + (exception == null ? 0 : exception.getClass().hashCode());
        result = 31 * result + (exception == null ? 0 : exception.getMessage().hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "ServerDescription{"
               + "address=" + address
               + ", type=" + type
               + ", state=" + state
               + (state == CONNECTED
                  ?
                  ", ok=" + ok
                  + ", version=" + version
                  + ", minWireVersion=" + minWireVersion
                  + ", maxWireVersion=" + maxWireVersion
                  + ", maxDocumentSize=" + maxDocumentSize
                  + ", roundTripTimeNanos=" + roundTripTimeNanos
                  : "")
               + (isReplicaSetMember()
                  ?
                  ", setName='" + setName + '\''
                  + ", canonicalAddress=" + canonicalAddress
                  + ", hosts=" + hosts
                  + ", passives=" + passives
                  + ", arbiters=" + arbiters
                  + ", primary='" + primary + '\''
                  + ", tagSet=" + tagSet
                  + ", electionId=" + electionId
                  + ", setVersion=" + setVersion
                  + ", lastWriteDate=" + lastWriteDate
                  + ", lastUpdateTimeNanos=" + lastUpdateTimeNanos
                  + ", logicalSessionTimeoutMinutes=" + logicalSessionTimeoutMinutes
                : "")
               + (exception == null ? "" : ", exception=" + translateExceptionToString())
               + '}';
    }

    /**
     * Returns a short, pretty description for this ServerDescription.
     *
     * @return a String containing the most pertinent information about this ServerDescription
     */
    public String getShortDescription() {
        return "{"
               + "address=" + address
               + ", type=" + type
               + (!tagSet.iterator().hasNext() ? "" : ", " + tagSet)
               + (state == CONNECTED ? (", roundTripTime=" + getRoundTripFormattedInMilliseconds() + " ms") : "")
               + ", state=" + state
               + (exception == null ? "" : ", exception=" + translateExceptionToString())
               + '}';
    }

    private String translateExceptionToString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append(exception);
        builder.append("}");
        Throwable cur = exception.getCause();
        while (cur != null) {
            builder.append(", caused by ");
            builder.append("{");
            builder.append(cur);
            builder.append("}");
            cur = cur.getCause();
        }

        return builder.toString();
    }


    private String getRoundTripFormattedInMilliseconds() {
        return new DecimalFormat("#0.0").format(roundTripTimeNanos / 1000.0 / 1000.0);
    }

    ServerDescription(final Builder builder) {
        address = notNull("address", builder.address);
        type = notNull("type", builder.type);
        state = notNull("state", builder.state);
        version = notNull("version", builder.version);
        canonicalAddress = builder.canonicalAddress;
        hosts = builder.hosts;
        passives = builder.passives;
        arbiters = builder.arbiters;
        primary = builder.primary;
        maxDocumentSize = builder.maxDocumentSize;
        tagSet = builder.tagSet;
        setName = builder.setName;
        roundTripTimeNanos = builder.roundTripTimeNanos;
        ok = builder.ok;
        minWireVersion = builder.minWireVersion;
        maxWireVersion = builder.maxWireVersion;
        electionId = builder.electionId;
        setVersion = builder.setVersion;
        lastWriteDate = builder.lastWriteDate;
        lastUpdateTimeNanos = builder.lastUpdateTimeNanos;
        logicalSessionTimeoutMinutes = builder.logicalSessionTimeoutMinutes;
        exception = builder.exception;
    }
}
