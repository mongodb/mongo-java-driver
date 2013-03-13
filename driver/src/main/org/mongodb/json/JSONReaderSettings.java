package org.mongodb.json;

import org.bson.BSONReaderSettings;
import org.mongodb.annotations.Immutable;

/**
 * Settings to control the behavior of a {@code JSONReader} instance.
 *
 * @see JSONWriter
 * @since 3.0.0
 */
@Immutable
public class JSONReaderSettings extends BSONReaderSettings {
    private final JSONMode inputMode;

    /**
     * Creates a new instance with default values for all properties.
     */
    public JSONReaderSettings() {
        this(JSONMode.Strict);
    }

    /**
     * Creates a new instance with the given output inputMode and default values for all other properties.
     *
     * @param mode the input mode
     */
    public JSONReaderSettings(final JSONMode mode) {
        this.inputMode = mode;
    }
}
