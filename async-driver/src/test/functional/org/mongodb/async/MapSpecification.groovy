package org.mongodb.async

import org.bson.types.ObjectId
import org.mongodb.Block
import org.mongodb.Document
import org.mongodb.Function

class MapSpecification extends FunctionalSpecification {

    def documents = [new Document('_id', new ObjectId()).append('x', 42), new Document('_id', new ObjectId()).append('x', 43)]

    def setup() {
        collection.insert(documents).get()
    }

    def 'should map source document into target document with into'() {
        expect:
        collection.find(new Document())
                  .map(new MappingFunction())
                  .into([]).get() == [new TargetDocument(documents[0]), new TargetDocument(documents[1])]
    }

    def 'should map source document into target document with forEach'() {
        when:
        def targetDocuments = []
        collection.find(new Document())
                  .map(new MappingFunction())
                  .forEach( { TargetDocument document -> targetDocuments += document } as Block<TargetDocument>).get()
        then:
        targetDocuments == [new TargetDocument(documents[0]), new TargetDocument(documents[1])]
    }

    def 'should map when already mapped'() {
        when:
        def targetIdStrings = []
        collection.find(new Document())
                  .map(new MappingFunction())
                  .map(new Function<TargetDocument, ObjectId>() {
            @Override
            ObjectId apply(final TargetDocument targetDocument) {
                targetDocument.getId()
            }
        }).forEach( { ObjectId id -> targetIdStrings += id.toString() } as Block<TargetDocument>).get()

        then:
        targetIdStrings == [new TargetDocument(documents[0]).getId().toString(), new TargetDocument(documents[1]).getId().toString()]
    }

    static class MappingFunction implements Function<Document, TargetDocument> {
        @Override
        TargetDocument apply(final Document document) {
            new TargetDocument(document)
        }
    }
}