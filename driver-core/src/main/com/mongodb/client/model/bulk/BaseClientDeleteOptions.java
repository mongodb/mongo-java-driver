package com.mongodb.client.model.bulk;

import com.mongodb.annotations.Sealed;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

@Sealed
interface BaseClientDeleteOptions {

    BaseClientDeleteOptions collation(@Nullable Collation collation);

    BaseClientDeleteOptions hint(@Nullable Bson hint);

    BaseClientDeleteOptions hintString(@Nullable String hintString);
}
