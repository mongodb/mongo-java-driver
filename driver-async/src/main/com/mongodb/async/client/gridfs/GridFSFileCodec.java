package com.mongodb.async.client.gridfs;

import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.assertions.Assertions;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.util.Date;

public class GridFSFileCodec implements CollectibleCodec<GridFSFile> {

    private static final String ID_FIELD_NAME = "_id";

    private final CodecRegistry registry;

    public GridFSFileCodec(final CodecRegistry registry) {
        this.registry = Assertions.notNull("registry", registry);
    }

    @Override
    public void generateIdIfAbsentFromDocument(final GridFSFile document) {
        if (!documentHasId(document)) {
            document.setId(new ObjectId());
        }
    }

    @Override
    public boolean documentHasId(final GridFSFile document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(final GridFSFile document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }
        return new BsonObjectId(document.getId());
    }

    @Override
    public GridFSFile decode(final BsonReader reader, final DecoderContext decoderContext) {

        reader.readStartDocument();

        GridFSFile gridFSFile = new GridFSFile();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();

            if (ID_FIELD_NAME.equals(fieldName)) {
                gridFSFile.setId(reader.readObjectId());
            } else if ("chunkSize".equals(fieldName)) {
                gridFSFile.setChunkSize(reader.readInt32());
            } else if ("contentType".equals(fieldName)) {
                gridFSFile.setContentType(reader.readString());
            } else if ("filename".equals(fieldName)) {
                gridFSFile.setFilename(reader.readString());
            } else if ("length".equals(fieldName)) {
                gridFSFile.setLength(reader.readInt64());
            } else if ("md5".equals(fieldName)) {
                gridFSFile.setMd5(reader.readString());
            } else if ("uploadDate".equals(fieldName)) {
                gridFSFile.setUploadDate(new Date(reader.readDateTime()));
            } else {
                reader.skipValue();
            }
        }
        //TODO metadata
        //TODO alias
        reader.readEndDocument();
        return gridFSFile;
    }

    @Override
    public void encode(final BsonWriter writer, final GridFSFile gridFSFile, final EncoderContext encoderContext) {

        writer.writeStartDocument();
        writer.writeObjectId(ID_FIELD_NAME, gridFSFile.getId());

        writer.writeInt32("chunkSize", gridFSFile.getChunkSize());
        if (gridFSFile.getContentType() != null) {
            writer.writeString("contentType", gridFSFile.getContentType());
        }
        if (gridFSFile.getFilename() != null) {
            writer.writeString("filename", gridFSFile.getFilename());
        }
        writer.writeInt64("length", gridFSFile.getLength());
        if (gridFSFile.getMd5() != null) {
            writer.writeString("md5", gridFSFile.getMd5());
        }
        writer.writeDateTime("uploadDate", gridFSFile.getUploadDate().getTime());
        writer.writeEndDocument();
        //TODO metadata
        //TODO alias
    }

    @Override
    public Class<GridFSFile> getEncoderClass() {
        return GridFSFile.class;
    }
}
