package org.bson.codecs;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.Symbol;

/**
 * A codec for BSON symbol type.
 *
 * @since 3.0
 */
public class SymbolCodec implements Codec<Symbol> {
    @Override
    public Symbol decode(final BSONReader reader) {
        return new Symbol(reader.readSymbol());
    }

    @Override
    public void encode(final BSONWriter writer, final Symbol value) {
        writer.writeSymbol(value.getSymbol());
    }

    @Override
    public Class<Symbol> getEncoderClass() {
        return Symbol.class;
    }
}
