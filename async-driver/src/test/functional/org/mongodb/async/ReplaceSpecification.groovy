package org.mongodb.async

import org.mongodb.Document

class ReplaceSpecification extends FunctionalSpecification {
     def 'should replace a document'() {
         given:
         collection.insert([new Document('_id', 1), new Document('_id', 2)]).get()
         def filter = new Document('_id', 2)
         def replacement = new Document('_id', 2).append('x', 1)

         when:
         collection.find(filter).replace(replacement).get()

         then:
         collection.find(filter).one().get() == replacement
     }
}