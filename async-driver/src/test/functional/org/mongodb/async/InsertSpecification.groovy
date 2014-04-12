package org.mongodb.async

import org.mongodb.Document

class InsertSpecification extends FunctionalSpecification {
    def 'should insert a document'() {
        given:
        def document = new Document('_id', 1)

        when:
        collection.insert(document).get()

        then:
        collection.find(new Document()).one().get() == document
    }

    def 'should insert documents'() {
        given:
        def documents = [new Document('_id', 1), new Document('_id', 2)]

        when:
        collection.insert(documents).get()

        then:
        collection.find(new Document()).into([]).get() == documents
    }
}