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

package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.result.CommandResult;
import org.mongodb.rs.Tag;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: Better unit test for this class
public class IsMasterCommandResult {
    private final boolean isPrimary;
    private final boolean isSecondary;
    private final List<String> hosts;
    private final List<String> passives;
    private final String primary;
    private final int maxBSONObjectSize;

    private final int maxMessageSize;
    private final Set<Tag> tags;
    private final String setName;
    private final long elapsedNanoseconds;
    private final boolean ok;


    public static class Builder {
        private boolean isPrimary;
        private boolean isSecondary;
        private List<String> hosts = Collections.emptyList();
        private List<String> passives = Collections.emptyList();
        private String primary;
        private int maxBSONObjectSize;
        private int maxMessageSize;
        private Set<Tag> tags = Collections.emptySet();
        private String setName;
        private long elapsedNanoseconds;
        private boolean ok;

        // CHECKSTYLE:OFF
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

        public Builder maxBSONObjectSize(final int maxBSONObjectSize) {
            this.maxBSONObjectSize = maxBSONObjectSize;
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

        public Builder setName(final String setName) {
            this.setName = setName;
            return this;
        }

        public Builder elapsedNanoseconds(final long elapsedNanoseconds) {
            this.elapsedNanoseconds = elapsedNanoseconds;
            return this;
        }

        public Builder ok(final boolean ok) {
            this.ok = ok;
            return this;
        }

        public IsMasterCommandResult build() {
            return new IsMasterCommandResult(this);
        }
        // CHECKSTYLE:OFF
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("unchecked")
    public IsMasterCommandResult(final CommandResult commandResult) {
        this(IsMasterCommandResult.builder()
                .primary((Boolean) commandResult.getResponse().get("ismaster"))
                .secondary(getIsSecondary((Boolean) commandResult.getResponse().get("secondary")))
                .hosts((List<String>) commandResult.getResponse().get("hosts"))
                .passives((List<String>) commandResult.getResponse().get("passives"))
                .primary((String) commandResult.getResponse().get("primary"))
                .maxBSONObjectSize((Integer) commandResult.getResponse().get("maxBsonObjectSize"))
                .maxMessageSize((Integer) commandResult.getResponse().get("maxMessageSizeBytes"))
                .tags(getTagsFromMap((Document) commandResult.getResponse().get("tags")))
                .elapsedNanoseconds(commandResult.getElapsedNanoseconds())
                .ok(commandResult.isOk()));
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public boolean isSecondary() {
        return isSecondary;
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

    public int getMaxBSONObjectSize() {
        return maxBSONObjectSize;
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

    public long getElapsedNanoseconds() {
        return elapsedNanoseconds;
    }

    public boolean isOk() {
        return ok;
    }

    IsMasterCommandResult(final Builder builder) {
        isPrimary = builder.isPrimary;
        isSecondary = builder.isSecondary;
        hosts = builder.hosts;
        passives = builder.passives;
        primary = builder.primary;
        maxBSONObjectSize = builder.maxBSONObjectSize;
        maxMessageSize = builder.maxMessageSize;
        tags = builder.tags;
        setName = builder.setName;
        elapsedNanoseconds = builder.elapsedNanoseconds;
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
}
