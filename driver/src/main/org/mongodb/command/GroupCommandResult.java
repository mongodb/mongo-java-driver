package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.CommandResult;

import java.util.List;

public class GroupCommandResult extends CommandResult {

    public GroupCommandResult(final CommandResult baseResult) {
        super(baseResult);
    }


    @SuppressWarnings("unchecked")
    public List<Document> getValue() {
        return (List<Document>) getResponse().get("retval");   // TODO: any way to remove the warning?  This could be a design flaw
    }
}
