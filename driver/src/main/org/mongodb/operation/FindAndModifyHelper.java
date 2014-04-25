package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Function;

final class FindAndModifyHelper {

    static <T> Function<CommandResult, T> transformer() {
        return new Function<CommandResult, T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T apply(final CommandResult result) {
                return (T) result.getResponse().get("value");
            }
        };
    }

    private FindAndModifyHelper() {
    }
}
