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

package com.mongodb;

import org.mongodb.annotations.Immutable;
import org.mongodb.connection.Tags;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that represents preferred replica set members to which a query or command can be sent.
 *
 * @mongodb.driver.manual applications/replication/#replica-set-read-preference  Read Preference
 */
@Immutable
public class ReadPreference {
    /**
     * A primary read preference.  Equivalent to calling {@code ReadPreference.primary()}.
     *
     * @see ReadPreference#primary()
     * @deprecated As of release 2.9.0, replaced by {@code ReadPreference.primary()}
     */
    @Deprecated
    public static final ReadPreference PRIMARY;

    /**
     * A secondary-preferred read preference.  Equivalent to calling {@code ReadPreference.secondaryPreferred}.  This
     * reference should really have been called {@code ReadPreference.SECONDARY_PREFERRED}, but the naming of it
     * preceded the idea of distinguishing between secondary and secondary-preferred, so for backwards compatibility,
     * leaving the name as is with the behavior as it was when it was created.
     *
     * @see ReadPreference#secondary()
     * @see ReadPreference#secondaryPreferred()
     * @deprecated As of release 2.9.0, replaced by {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public static final ReadPreference SECONDARY;


    private static final ReadPreference _PRIMARY;
    private static final ReadPreference _SECONDARY;
    private static final ReadPreference _SECONDARY_PREFERRED;
    private static final ReadPreference _PRIMARY_PREFERRED;
    private static final ReadPreference _NEAREST;

    private final org.mongodb.ReadPreference proxied;

    ReadPreference(final org.mongodb.ReadPreference proxied) {
        this.proxied = proxied;
    }

    org.mongodb.ReadPreference toNew() {
        return proxied;
    }

    public boolean isSlaveOk() {
        return proxied.isSlaveOk();
    }

    public DBObject toDBObject() {
        return DBObjects.toDBObject(proxied.toDocument());
    }

    public String getName() {
        return proxied.getName();
    }

    @Override
    public String toString() {
        return proxied.toString();
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
     * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
     */
    public static TaggableReadPreference primaryPreferred(final DBObject firstTagSet, final DBObject... remainingTagSets) {

        return new TaggableReadPreference(org.mongodb.ReadPreference.primaryPreferred(toTagsList(firstTagSet, remainingTagSets)));
    }

    private static List<Tags> toTagsList(final DBObject firstTagSet, final DBObject[] remainingTagSets) {
        List<Tags> tagsList = new ArrayList<Tags>();
        tagsList.add(toTagMap(firstTagSet));
        for (DBObject cur : remainingTagSets) {
            tagsList.add(toTagMap(cur));
        }
        return tagsList;
    }

    private static Tags toTagMap(final DBObject tagSet) {
        Tags tags = new Tags();
        for (String key : tagSet.keySet()) {
            tags.put(key, tagSet.get(key).toString());
        }
        return tags;
    }

    /**
     * @return ReadPreference which reads secondary.
     */
    public static ReadPreference secondary() {
        return _SECONDARY;
    }

    /**
     * @return ReadPreference which reads secondary respective of tags.
     */
    public static TaggableReadPreference secondary(final DBObject firstTagSet, final DBObject... remainingTagSets) {
        return new TaggableReadPreference(org.mongodb.ReadPreference.secondary(toTagsList(firstTagSet, remainingTagSets)));
    }

    /**
     * @return ReadPreference which reads secondary if available, otherwise from primary.
     */
    public static ReadPreference secondaryPreferred() {
        return _SECONDARY_PREFERRED;
    }

    /**
     * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective
     *         of tags.
     */
    public static TaggableReadPreference secondaryPreferred(final DBObject firstTagSet, final DBObject... remainingTagSets) {
        return new TaggableReadPreference(org.mongodb.ReadPreference.secondaryPreferred(toTagsList(firstTagSet, remainingTagSets)));
    }

    /**
     * @return ReadPreference which reads nearest node.
     */
    public static ReadPreference nearest() {
        return _NEAREST;
    }

    /**
     * @return ReadPreference which reads nearest node respective of tags.
     */
    public static TaggableReadPreference nearest(final DBObject firstTagSet, final DBObject... remainingTagSets) {
        return new TaggableReadPreference(org.mongodb.ReadPreference.nearest(toTagsList(firstTagSet, remainingTagSets)));
    }

    public static ReadPreference valueOf(final String name) {
        return fromNew(org.mongodb.ReadPreference.valueOf(name));
    }

    public static TaggableReadPreference valueOf(final String name, final DBObject firstTagSet,
                                                 final DBObject... remainingTagSets) {
        return (TaggableReadPreference) fromNew(org.mongodb.ReadPreference.valueOf(name, toTagsList(firstTagSet, remainingTagSets)));
    }

    public static ReadPreference fromNew(final org.mongodb.ReadPreference readPreference) {
        if (readPreference instanceof org.mongodb.TaggableReadPreference) {
            return new TaggableReadPreference((org.mongodb.TaggableReadPreference) readPreference);
        }
        else {
            return new ReadPreference(readPreference);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ReadPreference that = (ReadPreference) o;

        if (!proxied.equals(that.proxied)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return proxied.hashCode();
    }

    static {
        _PRIMARY = fromNew(org.mongodb.ReadPreference.primary());
        _SECONDARY = fromNew(org.mongodb.ReadPreference.secondary());
        _SECONDARY_PREFERRED = fromNew(org.mongodb.ReadPreference.secondaryPreferred());
        _PRIMARY_PREFERRED = fromNew(org.mongodb.ReadPreference.primaryPreferred());
        _NEAREST = fromNew(org.mongodb.ReadPreference.nearest());

        PRIMARY = _PRIMARY;
        SECONDARY = _SECONDARY_PREFERRED;  // this is not a bug.  See SECONDARY Javadoc.

    }

}
