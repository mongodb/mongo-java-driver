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

package org.mongodb.connection;

import org.mongodb.Document;
import org.mongodb.operation.CommandResult;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mongodb.assertions.Assertions.notNull;

public class ServerDescription {
    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB

    private final ServerAddress address;

    private final boolean isPrimary;
    private final boolean isSecondary;
    private final List<String> hosts;
    private final List<String> passives;
    private final String primary;
    private final int maxDocumentSize;

    private final int maxMessageSize;
    private final Set<Tag> tags;
    private final String setName;
    private final long averagePingTime;
    private final boolean ok;

    public static class Builder {
        private ServerAddress address;
        private boolean isPrimary;
        private boolean isSecondary;
        private List<String> hosts = Collections.emptyList();
        private List<String> passives = Collections.emptyList();
        private String primary;
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private Set<Tag> tags = Collections.emptySet();
        private String setName;
        private long averagePingTime;
        private boolean ok;
        private ServerDescription previous;

        // CHECKSTYLE:OFF
        public Builder address(final ServerAddress address) {
            this.address = address;
            return this;
        }

        public Builder primary(final boolean isPrimary) {
            this.isPrimary = isPrimary;
            return this;
        }

        public Builder secondary(final boolean isSecondary) {
            this.isSecondary = isSecondary;
            return this;
        }

        public Builder hosts(final List<String> hosts) {
            this.hosts = hosts == null ? Collections.<String>emptyList() : hosts;
            return this;
        }

        public Builder passives(final List<String> passives) {
            this.passives = passives == null ? Collections.<String>emptyList() : passives;
            return this;
        }

        public Builder primary(final String primary) {
            this.primary = primary;
            return this;
        }

        public Builder maxDocumentSize(final int maxBSONObjectSize) {
            this.maxDocumentSize = maxBSONObjectSize;
            return this;
        }

        public Builder maxMessageSize(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public Builder tags(final Set<Tag> tags) {
            this.tags = tags == null ? Collections.<Tag>emptySet() : tags;
            return this;
        }

        public Builder averagePingTime(final long averagePingTime) {
            this.averagePingTime = averagePingTime;
            return this;
        }

        public Builder setName(final String setName) {
            this.setName = setName;
            return this;
        }

        public Builder ok(final boolean ok) {
            this.ok = ok;
            return this;
        }

        public ServerDescription build() {
            return new ServerDescription(this);
        }
        // CHECKSTYLE:OFF
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("unchecked")
    public ServerDescription(final CommandResult commandResult, final long averagePingTime) {
        this(ServerDescription.builder()
                .address(commandResult.getAddress())
                .primary((Boolean) commandResult.getResponse().get("ismaster"))
                .secondary(getIsSecondary((Boolean) commandResult.getResponse().get("secondary")))
                .hosts((List<String>) commandResult.getResponse().get("hosts"))
                .passives((List<String>) commandResult.getResponse().get("passives"))
                .primary((String) commandResult.getResponse().get("primary"))
                .maxDocumentSize((Integer) commandResult.getResponse().get("maxBsonObjectSize"))
                .maxMessageSize(getMaxMessageSize((Integer) commandResult.getResponse().get("maxMessageSizeBytes")))
                .tags(getTagsFromMap((Document) commandResult.getResponse().get("tags")))
                .averagePingTime(averagePingTime)
                .ok(commandResult.isOk()));
    }

    public ServerAddress getAddress() {
        return address;
    }

    public boolean isPrimary() {
        return ok && isPrimary;
    }

    public boolean isSecondary() {
        return ok && isSecondary;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public List<String> getPassives() {
        return passives;
    }

    public String getPrimary() {
        return primary;
    }

    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public Set<Tag> getTags() {
        return tags;
    }

    public String getSetName() {
        return setName;
    }

    public boolean isOk() {
        return ok;
    }

    public long getAveragePingTime() {
        return averagePingTime;
    }

    public float getAveragePingTimeMillis() {
        return averagePingTime / 1000000F;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ServerDescription that = (ServerDescription) o;

        if (Long.compare(that.averagePingTime, averagePingTime) != 0) return false;
        if (isPrimary != that.isPrimary) return false;
        if (isSecondary != that.isSecondary) return false;
        if (maxDocumentSize != that.maxDocumentSize) return false;
        if (maxMessageSize != that.maxMessageSize) return false;
        if (ok != that.ok) return false;
        if (!hosts.equals(that.hosts)) return false;
        if (!passives.equals(that.passives)) return false;
        if (primary != null ? !primary.equals(that.primary) : that.primary != null) return false;
        if (!address.equals(that.address)) return false;
        if (setName != null ? !setName.equals(that.setName) : that.setName != null) return false;
        if (!tags.equals(that.tags)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + (isPrimary ? 1 : 0);
        result = 31 * result + (isSecondary ? 1 : 0);
        result = 31 * result + hosts.hashCode();
        result = 31 * result + passives.hashCode();
        result = 31 * result + (primary != null ? primary.hashCode() : 0);
        result = 31 * result + maxDocumentSize;
        result = 31 * result + maxMessageSize;
        result = 31 * result + tags.hashCode();
        result = 31 * result + (setName != null ? setName.hashCode() : 0);
        result = 31 * result + (int) (averagePingTime ^ (averagePingTime >>> 32));
        result = 31 * result + (ok ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ServerDescription{"
                + "address=" + address
                + ", isPrimary=" + isPrimary
                + ", isSecondary=" + isSecondary
                + ", hosts=" + hosts
                + ", passives=" + passives
                + ", primary='" + primary + '\''
                + ", maxDocumentSize=" + maxDocumentSize
                + ", maxMessageSize=" + maxMessageSize
                + ", tags=" + tags
                + ", setName='" + setName + '\''
                + ", averagePingTime=" + averagePingTime
                + ", ok=" + ok
                + '}';
    }

    ServerDescription(final Builder builder) {
        notNull("address", builder.address);

        address = builder.address;
        isPrimary = builder.isPrimary;
        isSecondary = builder.isSecondary;
        hosts = builder.hosts;
        passives = builder.passives;
        primary = builder.primary;
        maxDocumentSize = builder.maxDocumentSize;
        maxMessageSize = builder.maxMessageSize;
        tags = builder.tags;
        setName = builder.setName;
        averagePingTime = builder.averagePingTime;
        ok = builder.ok;
    }

    private static boolean getIsSecondary(final Boolean isSecondary) {
        return isSecondary == null ? false : isSecondary;
    }

    private static Set<Tag> getTagsFromMap(final Document tagsDocuments) {
        if (tagsDocuments == null) {
            return Collections.emptySet();
        }
        final Set<Tag> tagSet = new HashSet<Tag>();
        for (final Map.Entry<String, Object> curEntry : tagsDocuments.entrySet()) {
            tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue().toString()));
        }
        return tagSet;
    }

    private static int getMaxMessageSize(final Integer maxMessageSize) {
        return (maxMessageSize != null) ? maxMessageSize : DEFAULT_MAX_MESSAGE_SIZE;
    }

}
