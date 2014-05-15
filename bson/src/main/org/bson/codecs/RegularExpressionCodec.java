package org.bson.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.RegularExpression;

/**
 * A codec for BSON regular expressions.
 *
 * @since 3.0
 */
public class RegularExpressionCodec implements Codec<RegularExpression> {
    @Override
    public RegularExpression decode(final BSONReader reader) {
        return reader.readRegularExpression();
    }

    @Override
    public void encode(final BSONWriter writer, final RegularExpression value) {
        writer.writeRegularExpression(value);
    }

    @Override
    public Class<RegularExpression> getEncoderClass() {
        return RegularExpression.class;
    }
}
