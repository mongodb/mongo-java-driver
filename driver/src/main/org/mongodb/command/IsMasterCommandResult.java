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
 *
 */

package org.mongodb.command;

import org.bson.types.Document;
import org.mongodb.result.CommandResult;

import java.util.List;

public class IsMasterCommandResult extends CommandResult {
    public IsMasterCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }

    public boolean isMaster() {
        return (Boolean) getResponse().get("ismaster");
    }

    public boolean isSecondary() {
        Boolean isSecondary = (Boolean) getResponse().get("secondary");
        return isSecondary == null ? false : isSecondary;
    }

    @SuppressWarnings("unchecked")
    public List<String> getHosts() {
        return (List<String>) getResponse().get("hosts");
    }

    @SuppressWarnings("unchecked")
    public List<String> getPassives() {
        return (List<String>) getResponse().get("passives");
    }

    public String getPrimary() {
        return (String) getResponse().get("primary");
    }

    public int getMaxBsonObjectSize() {
        return (Integer) getResponse().get("maxBsonObjectSize");
    }

    public Document getTags() {
        return (Document) getResponse().get("tags");
    }

    public String getSetName() {
        return (String) getResponse().get("setName");
    }
}
