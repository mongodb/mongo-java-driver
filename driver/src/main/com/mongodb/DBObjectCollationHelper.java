/*
 * Copyright 2016 MongoDB, Inc.
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

import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationStrength;

final class DBObjectCollationHelper {

    static Collation createOptions(final DBObject options) {
        if (options.get("collation") == null) {
            return null;
        } else if (!(options.get("collation") instanceof DBObject)) {
            throw new IllegalArgumentException("collation options should be a document");
        } else {
            Collation.Builder builder = Collation.builder();
            DBObject collation = (DBObject) options.get("collation");

            if (collation.get("locale") == null) {
                throw new IllegalArgumentException("'locale' is required when providing collation options");
            } else {
                Object locale = collation.get("locale");
                if (!(locale instanceof String)) {
                    throw new IllegalArgumentException("collation 'locale' should be a String");
                } else {
                    builder.locale((String) locale);
                }
            }
            if (collation.get("caseLevel") != null){
                Object caseLevel = collation.get("caseLevel");
                if (!(caseLevel instanceof Boolean)) {
                    throw new IllegalArgumentException("collation 'caseLevel' should be a Boolean");
                } else {
                    builder.caseLevel((Boolean) caseLevel);
                }
            }
            if (collation.get("caseFirst") != null) {
                Object caseFirst = collation.get("caseFirst");
                if (!(caseFirst instanceof String)) {
                    throw new IllegalArgumentException("collation 'caseFirst' should be a String");
                } else {
                    builder.collationCaseFirst(CollationCaseFirst.fromString((String) caseFirst));
                }
            }
            if (collation.get("strength") != null) {
                Object strength = collation.get("strength");
                if (!(strength instanceof Integer)) {
                    throw new IllegalArgumentException("collation 'strength' should be an Integer");
                } else {
                    builder.collationStrength(CollationStrength.fromInt((Integer) strength));
                }
            }
            if (collation.get("numericOrdering") != null) {
                Object numericOrdering = collation.get("numericOrdering");
                if (!(numericOrdering instanceof Boolean)) {
                    throw new IllegalArgumentException("collation 'numericOrdering' should be a Boolean");
                } else {
                    builder.numericOrdering((Boolean) numericOrdering);
                }
            }
            if (collation.get("alternate") != null) {
                Object alternate = collation.get("alternate");
                if (!(alternate instanceof String)) {
                    throw new IllegalArgumentException("collation 'alternate' should be a String");
                } else {
                    builder.collationAlternate(CollationAlternate.fromString((String) alternate));
                }
            }
            if (collation.get("maxVariable") != null) {
                Object maxVariable = collation.get("maxVariable");
                if (!(maxVariable instanceof String)) {
                    throw new IllegalArgumentException("collation 'maxVariable' should be a String");
                } else {
                    builder.collationMaxVariable(CollationMaxVariable.fromString((String) maxVariable));
                }
            }
            if (collation.get("backwards") != null) {
                Object backwards = collation.get("backwards");
                if (!(backwards instanceof Boolean)) {
                    throw new IllegalArgumentException("collation 'backwards' should be a Boolean");
                } else {
                    builder.backwards((Boolean) backwards);
                }
            }
            return builder.build();
        }
    }

    private DBObjectCollationHelper() {
    }
}
