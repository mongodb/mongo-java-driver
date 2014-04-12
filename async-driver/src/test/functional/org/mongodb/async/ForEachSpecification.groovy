package org.mongodb.async

import org.mongodb.Block
import org.mongodb.Document
import org.mongodb.MongoInternalException

class ForEachSpecification extends FunctionalSpecification {
    def 'should complete with no results'() {
        expect:
        collection.find(new Document()).forEach( { } as Block).get() == null
    }

    def 'should apply block and complete'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == [document]
    }

    def 'should apply block for each document and then complete'() {
        given:
        def documents = [new Document(), new Document()]
        collection.insert(documents[0]).get()
        collection.insert(documents[1]).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == documents
    }

    def 'should throw MongoInternalException if apply throws'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        collection.find(new Document()).forEach( { doc -> throw new IllegalArgumentException() } as Block).get()

        then:
        thrown(MongoInternalException)
    }
}