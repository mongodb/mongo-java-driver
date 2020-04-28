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

import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A commit quorum specifies how many data-bearing members of a replica set, including the primary, must
 * complete the index builds successfully before the primary marks the indexes as ready.
 *
 * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
 * @mongodb.server.release 4.4
 * @since 4.1
 */
public abstract class CreateIndexCommitQuorum {

    /**
     * A create index commit quorum of majority.
     */
    public static final CreateIndexCommitQuorum MAJORITY = new CreateIndexCommitQuorumWithMode("majority");

    /**
     * A create index commit quorum of voting members.
     */
    public static final CreateIndexCommitQuorum VOTING_MEMBERS = new CreateIndexCommitQuorumWithMode("votingMembers");

    /**
     * Create a create index commit quorum with a mode value.
     *
     * @param mode the mode value
     * @return a create index commit quorum of the specified mode
     */
    public static CreateIndexCommitQuorum create(final String mode) {
        return new CreateIndexCommitQuorumWithMode(mode);
    }

    /**
     * Create a create index commit quorum with a w value.
     *
     * @param w the w value
     * @return a create index commit quorum with the specified w value
     */
    public static CreateIndexCommitQuorum create(final int w) {
        return new CreateIndexCommitQuorumWithW(w);
    }

    /**
     * Converts the create index commit quorum to a Bson value.
     *
     * @return the BsonValue that represents the create index commit quorum
     */
    public abstract BsonValue toBsonValue();

    private CreateIndexCommitQuorum() {
    }

    private static final class CreateIndexCommitQuorumWithMode extends CreateIndexCommitQuorum {
        private final String mode;

        private CreateIndexCommitQuorumWithMode(final String mode) {
            notNull("mode", mode);
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }

        @Override
        public BsonValue toBsonValue() {
            return new BsonString(mode);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CreateIndexCommitQuorumWithMode that = (CreateIndexCommitQuorumWithMode) o;
            return mode.equals(that.mode);
        }

        @Override
        public int hashCode() {
            return mode.hashCode();
        }

        @Override
        public String toString() {
            return "CreateIndexCommitQuorum{"
                    + "mode=" + mode
                    + '}';
        }
    }

    private static final class CreateIndexCommitQuorumWithW extends CreateIndexCommitQuorum {
        private final int w;

        private CreateIndexCommitQuorumWithW(final int w) {
            if (w < 0) {
                throw new IllegalArgumentException("w cannot be less than zero");
            }
            this.w = w;
        }

        public int getW() {
            return w;
        }

        @Override
        public BsonValue toBsonValue() {
            return new BsonInt32(w);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CreateIndexCommitQuorumWithW that = (CreateIndexCommitQuorumWithW) o;
            return w == that.w;
        }

        @Override
        public int hashCode() {
            return w;
        }

        @Override
        public String toString() {
            return "CreateIndexCommitQuorum{"
                    + "w=" + w
                    + '}';
        }
    }
}
