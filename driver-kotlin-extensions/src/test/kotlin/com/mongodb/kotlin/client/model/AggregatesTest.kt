/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2016/2022 Litote
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @custom-license-header
 */
package com.mongodb.kotlin.client.model

import com.mongodb.MongoNamespace
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Aggregates.project
import com.mongodb.client.model.GraphLookupOptions
import com.mongodb.client.model.MergeOptions
import com.mongodb.client.model.QuantileMethod
import com.mongodb.client.model.UnwindOptions
import com.mongodb.client.model.densify.DensifyOptions
import com.mongodb.client.model.densify.DensifyRange
import com.mongodb.kotlin.client.MongoCollection
import com.mongodb.kotlin.client.model.Accumulators.accumulator
import com.mongodb.kotlin.client.model.Accumulators.addToSet
import com.mongodb.kotlin.client.model.Accumulators.avg
import com.mongodb.kotlin.client.model.Accumulators.bottom
import com.mongodb.kotlin.client.model.Accumulators.bottomN
import com.mongodb.kotlin.client.model.Accumulators.first
import com.mongodb.kotlin.client.model.Accumulators.firstN
import com.mongodb.kotlin.client.model.Accumulators.last
import com.mongodb.kotlin.client.model.Accumulators.lastN
import com.mongodb.kotlin.client.model.Accumulators.max
import com.mongodb.kotlin.client.model.Accumulators.maxN
import com.mongodb.kotlin.client.model.Accumulators.median
import com.mongodb.kotlin.client.model.Accumulators.mergeObjects
import com.mongodb.kotlin.client.model.Accumulators.min
import com.mongodb.kotlin.client.model.Accumulators.minN
import com.mongodb.kotlin.client.model.Accumulators.percentile
import com.mongodb.kotlin.client.model.Accumulators.push
import com.mongodb.kotlin.client.model.Accumulators.stdDevPop
import com.mongodb.kotlin.client.model.Accumulators.stdDevSamp
import com.mongodb.kotlin.client.model.Accumulators.sum
import com.mongodb.kotlin.client.model.Accumulators.top
import com.mongodb.kotlin.client.model.Accumulators.topN
import com.mongodb.kotlin.client.model.Aggregates.count
import com.mongodb.kotlin.client.model.Aggregates.densify
import com.mongodb.kotlin.client.model.Aggregates.graphLookup
import com.mongodb.kotlin.client.model.Aggregates.lookup
import com.mongodb.kotlin.client.model.Aggregates.merge
import com.mongodb.kotlin.client.model.Aggregates.out
import com.mongodb.kotlin.client.model.Aggregates.unionWith
import com.mongodb.kotlin.client.model.Aggregates.unwind
import com.mongodb.kotlin.client.model.Projections.excludeId
import com.mongodb.kotlin.client.model.Projections.projection
import com.mongodb.kotlin.client.model.Sorts.ascending
import kotlin.test.assertEquals
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.verify
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class AggregatesTest {

    companion object {
        @Mock internal val wrappedEmployee: com.mongodb.client.MongoCollection<Employee> = mock()
        @Mock
        internal val wrappedEmployeeCoroutine: com.mongodb.reactivestreams.client.MongoCollection<Employee> = mock()
        @Mock internal val wrappedCustomer: com.mongodb.client.MongoCollection<Customer> = mock()
        @Mock
        internal val wrappedCustomerCoroutine: com.mongodb.reactivestreams.client.MongoCollection<Customer> = mock()

        lateinit var employeeCollection: MongoCollection<Employee>
        lateinit var employeeCollectionCoroutine: com.mongodb.kotlin.client.coroutine.MongoCollection<Employee>
        lateinit var customerCollection: MongoCollection<Customer>
        lateinit var customerCollectionCoroutine: com.mongodb.kotlin.client.coroutine.MongoCollection<Customer>

        @JvmStatic
        @BeforeAll
        internal fun setUpMocks() {
            employeeCollection = MongoCollection(wrappedEmployee)
            employeeCollectionCoroutine = com.mongodb.kotlin.client.coroutine.MongoCollection(wrappedEmployeeCoroutine)

            customerCollection = MongoCollection(wrappedCustomer)
            customerCollectionCoroutine = com.mongodb.kotlin.client.coroutine.MongoCollection(wrappedCustomerCoroutine)

            whenever(wrappedEmployee.namespace).doReturn(MongoNamespace("db", Employee::class.simpleName!!))
            whenever(wrappedEmployeeCoroutine.namespace).doReturn(MongoNamespace("db", Employee::class.simpleName!!))
            whenever(wrappedCustomer.namespace).doReturn(MongoNamespace("db", Customer::class.simpleName!!))
            whenever(wrappedCustomerCoroutine.namespace).doReturn(MongoNamespace("db", Customer::class.simpleName!!))

            employeeCollection.namespace
            verify(wrappedEmployee).namespace
            assertEquals(Employee::class.simpleName, employeeCollection.namespace.collectionName)

            employeeCollectionCoroutine.namespace
            verify(wrappedEmployeeCoroutine).namespace
            assertEquals(Employee::class.simpleName, employeeCollectionCoroutine.namespace.collectionName)

            customerCollection.namespace
            verify(wrappedCustomer).namespace
            assertEquals(Customer::class.simpleName, customerCollection.namespace.collectionName)

            customerCollectionCoroutine.namespace
            verify(wrappedCustomerCoroutine).namespace
            assertEquals(Customer::class.simpleName, customerCollectionCoroutine.namespace.collectionName)
        }
    }

    @Test
    fun count() {
        assertEquals(""" {${'$'}count: "name"}""", count(Person::name))
    }

    @Test
    fun lookup() {
        assertEquals(
            """ {"${'$'}lookup":
                {"from": "Customer", "localField": "customerId", "foreignField": "customerId", "as": "invoice"}}""",
            lookup(customerCollection, Order::customerId, Customer::customerId, "invoice"))
        assertEquals(
            Aggregates.lookup("Customer", "customerId", "customerId", "invoice"),
            lookup(customerCollection, Order::customerId, Customer::customerId, "invoice"))

        assertEquals(
            """ {"${'$'}lookup":
                {"from": "Customer", "localField": "customerId", "foreignField": "customerId", "as": "invoice"}}""",
            lookup(customerCollectionCoroutine, Order::customerId, Customer::customerId, "invoice"))
        assertEquals(
            Aggregates.lookup("Customer", "customerId", "customerId", "invoice"),
            lookup(customerCollectionCoroutine, Order::customerId, Customer::customerId, "invoice"))
    }

    @Test
    fun graphLookup() {
        assertEquals(
            """ {"${'$'}graphLookup":
                 {"from": "Employee", "startWith": "${'$'}id", "connectFromField": "id", "connectToField":
            "reportsTo", "as": "subordinates", "maxDepth": 1}}
  """,
            graphLookup(
                from = employeeCollection,
                startWith = Employee::id.projection,
                connectFromField = Employee::id,
                connectToField = Employee::reportsTo,
                fieldAs = "subordinates",
                options = GraphLookupOptions().maxDepth(1)))

        assertEquals(
            """ {"${'$'}graphLookup":
                 {"from": "Employee", "startWith": "${'$'}id", "connectFromField": "id", "connectToField":
            "reportsTo", "as": "subordinates", "maxDepth": 1}}
  """,
            graphLookup(
                from = employeeCollectionCoroutine,
                startWith = Employee::id.projection,
                connectFromField = Employee::id,
                connectToField = Employee::reportsTo,
                fieldAs = "subordinates",
                options = GraphLookupOptions().maxDepth(1)))
    }

    @Test
    fun unionWith() {
        assertEquals(
            """  {"${'$'}unionWith": {"coll": "Customer", "pipeline": [{"${'$'}project": {"_id": 0}}]}} """,
            unionWith(collection = customerCollection, pipeline = listOf(project(excludeId()))))

        assertEquals(
            """  {"${'$'}unionWith": {"coll": "Customer", "pipeline": [{"${'$'}project": {"_id": 0}}]}} """,
            unionWith(collection = customerCollectionCoroutine, pipeline = listOf(project(excludeId()))))
    }

    @Test
    fun unwind() {
        assertEquals(UnwindOptions(), UnwindOptions())
        assertEquals("""  {"${'$'}unwind": "${'$'}address"} """, unwind(Person::address))

        assertEquals(
            """ {"${'$'}unwind":
                {"path": "${'$'}address", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "idx"}} """,
            unwind(Person::address, UnwindOptions().includeArrayIndex("idx").preserveNullAndEmptyArrays(true)))
    }

    @Test
    fun out() {
        assertEquals(""" {"${'$'}out": "Employee"}  """, out(employeeCollection))
        assertEquals(""" {"${'$'}out": "Employee"}  """, out(employeeCollectionCoroutine))
    }

    @Test
    fun merge() {
        assertEquals(""" {"${'$'}merge": {"into": "Customer"}} """, merge(customerCollection))
        assertEquals(""" {"${'$'}merge": {"into": "Customer"}} """, merge(customerCollectionCoroutine))

        assertEquals(
            """ {"${'$'}merge": {"into": "Customer", "on": "ssn"}} """,
            merge(customerCollection, MergeOptions().uniqueIdentifier("ssn")))
        assertEquals(
            """ {"${'$'}merge": {"into": "Customer", "on": "ssn"}} """,
            merge(customerCollectionCoroutine, MergeOptions().uniqueIdentifier("ssn")))
    }

    @Test
    fun densify() {
        assertEquals(
            """  {"${'$'}densify": {
                    "field": "email",
                    "range": { "bounds": "full", "step": 1 }
                }} """,
            densify(Customer::email, DensifyRange.fullRangeWithStep(1)))

        assertEquals(
            """  {"${'$'}densify": {
                    "field": "email",
                    "range": { "bounds": "full", "step": 1 },
                    "partitionByFields": ["foo"]
                }} """,
            densify(
                Customer::email,
                range = DensifyRange.fullRangeWithStep(1),
                options = DensifyOptions.densifyOptions().partitionByFields("foo")))
    }

    @Test
    @Suppress("LongMethod")
    fun accumulators() {
        assertEquals(com.mongodb.client.model.Accumulators.sum("age", 1), sum(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.avg("age", 1), avg(Person::age, 1))

        assertEquals(
            com.mongodb.client.model.Accumulators.percentile("age", 1, 2, QuantileMethod.approximate()),
            percentile(Person::age, 1, 2, QuantileMethod.approximate()))

        assertEquals(
            com.mongodb.client.model.Accumulators.median("age", 1, QuantileMethod.approximate()),
            median(Person::age, 1, QuantileMethod.approximate()))

        assertEquals(com.mongodb.client.model.Accumulators.first("age", 1), first(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.firstN("age", 1, 2), firstN(Person::age, 1, 2))

        assertEquals(
            com.mongodb.client.model.Accumulators.top("age", com.mongodb.client.model.Sorts.ascending("name"), 1),
            top(Person::age, ascending(Person::name), 1))

        assertEquals(
            com.mongodb.client.model.Accumulators.topN("age", com.mongodb.client.model.Sorts.ascending("name"), 1, 2),
            topN(Person::age, ascending(Person::name), 1, 2))

        assertEquals(com.mongodb.client.model.Accumulators.last("age", 1), last(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.lastN("age", 1, 2), lastN(Person::age, 1, 2))

        assertEquals(
            com.mongodb.client.model.Accumulators.bottom("age", com.mongodb.client.model.Sorts.ascending("name"), 1),
            bottom(Person::age, ascending(Person::name), 1))

        assertEquals(
            com.mongodb.client.model.Accumulators.bottomN(
                "age", com.mongodb.client.model.Sorts.ascending("name"), 1, 2),
            bottomN(Person::age, ascending(Person::name), 1, 2))

        assertEquals(com.mongodb.client.model.Accumulators.max("age", 1), max(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.maxN("age", 1, 2), maxN(Person::age, 1, 2))

        assertEquals(com.mongodb.client.model.Accumulators.min("age", 1), min(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.minN("age", 1, 2), minN(Person::age, 1, 2))

        assertEquals(com.mongodb.client.model.Accumulators.push("age", 1), push(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.addToSet("age", 1), addToSet(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.mergeObjects("age", 1), mergeObjects(Person::age, 1))

        assertEquals(
            com.mongodb.client.model.Accumulators.accumulator(
                "age", "initFunction", "accumulateFunction", "mergeFunction"),
            accumulator(Person::age, "initFunction", "accumulateFunction", "mergeFunction"))

        assertEquals(
            com.mongodb.client.model.Accumulators.accumulator(
                "age", "initFunction", "accumulateFunction", "mergeFunction", "finalizeFunction"),
            accumulator(Person::age, "initFunction", "accumulateFunction", "mergeFunction", "finalizeFunction"))

        assertEquals(
            com.mongodb.client.model.Accumulators.accumulator(
                "age",
                "initFunction",
                listOf("a", "b"),
                "accumulateFunction",
                listOf("c", "d"),
                "mergeFunction",
                "finalizeFunction"),
            accumulator(
                Person::age,
                "initFunction",
                listOf("a", "b"),
                "accumulateFunction",
                listOf("c", "d"),
                "mergeFunction",
                "finalizeFunction"))

        assertEquals(
            com.mongodb.client.model.Accumulators.accumulator(
                "age", "initFunction", "accumulateFunction", "mergeFunction", "finalizeFunction", "Kotlin"),
            accumulator(
                Person::age, "initFunction", "accumulateFunction", "mergeFunction", "finalizeFunction", "Kotlin"))

        assertEquals(
            com.mongodb.client.model.Accumulators.accumulator(
                "age",
                "initFunction",
                listOf("a", "b"),
                "accumulateFunction",
                listOf("c", "d"),
                "mergeFunction",
                "finalizeFunction",
                "Kotlin"),
            accumulator(
                Person::age,
                "initFunction",
                listOf("a", "b"),
                "accumulateFunction",
                listOf("c", "d"),
                "mergeFunction",
                "finalizeFunction",
                "Kotlin"))

        assertEquals(com.mongodb.client.model.Accumulators.stdDevPop("age", 1), stdDevPop(Person::age, 1))

        assertEquals(com.mongodb.client.model.Accumulators.stdDevSamp("age", 1), stdDevSamp(Person::age, 1))
    }

    data class Person(val name: String, val age: Int, val address: List<String>?, val results: List<Int>)
    data class Employee(val id: String, val name: String, val reportsTo: String)
    data class Order(val id: String, val orderId: String, val customerId: Int, val amount: Int)
    data class Customer(val id: String, val customerId: Int, val name: String, val email: String?)

    private fun assertEquals(expected: String, result: Bson) =
        assertEquals(BsonDocument.parse(expected), result.toBsonDocument())
}
