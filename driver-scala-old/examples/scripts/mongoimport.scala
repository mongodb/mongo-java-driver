#!/bin/sh
L=`pwd`
cp=`echo $L/lib/*`
exec scala -cp "$cp" "$0" "$@"
!#

/*
 * Copyright 2008-present MongoDB, Inc.
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
 */

import java.util.concurrent.TimeUnit
import java.util.logging.Level

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.io.{BufferedSource, Source}

import com.mongodb.ConnectionString

import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{Completed, MongoClient, MongoCollection}

/**
 * An example program providing similar functionality as the `mongoimport` program
 *
 * As there is no core CSV library for Scala CSV import is an exercise left to the reader
 *
 * Add mongo-scala-driver-alldep jar to your path or add to ./lib directory and then run as a shell program:
 *
 * {{{
 *    ./mongoimport.scala -u mongodb://localhost/test.testData --drop < data/testData.json
 * }}}
 *
 * Alternatively, run the `main` method in an IDE and pass the arguments in.
 *
 */
object mongoimport {

  val usage = """
                |Import JSON data into MongoDB using Casbah
                |
                |When importing JSON documents, each document must be a separate line of the input file.
                |
                |Example:
                |  mongoimport --uri mongodb://localhost/my_db.my_collection < mydocfile.json
                |
                |Options:
                |  --help                        produce help message
                |  --quiet                       silence all non error diagnostic messages
                |  -u [ --uri ] arg              The connection URI - must contain a collection
                |                                mongodb://[username:password@]host1[:port1][,host2[:port2]]/database.collection[?options]
                |                                See: http://docs.mongodb.org/manual/reference/connection-string/
                |  --file arg                    file to import from; if not specified stdin is used
                |  --drop                        drop collection first
              """.stripMargin

  /**
   * The main export program
   * @param args the commandline arguments
   */
  def main(args: Array[String]) {

    // The time when the execution of this program started, in milliseconds since 1 January 1970 UTC.
    val executionStart: Long = currentTime


    if (args.length == 0 | args.contains("--help")) {
      Console.err.println(usage)
      sys.exit(1)
    }

    // Set the debug log level
    java.util.logging.Logger.getLogger("").getHandlers.foreach(h => h.setLevel(Level.WARNING))

    val optionMap = parseArgs(Map(), args.toList)
    val options = getOptions(optionMap)

    if (options.uri.isEmpty) {
      Console.err.println(s"Missing URI")
      Console.err.println(usage)
      sys.exit(1)
    }

    // Get source
    val importSource: BufferedSource = options.file match {
      case None => Source.stdin
      case Some(fileName) => Source.fromFile(fileName)
    }

    // Get URI
    val mongoClientURI = new ConnectionString(options.uri.get)
    if (Option(mongoClientURI.getCollection).isEmpty) {
      Console.err.println(s"Missing collection name in the URI eg:  mongodb://<hostInformation>/<database>.<collection>[?options]")
      Console.err.println(s"Current URI: $mongoClientURI")
      sys.exit(1)
    }

    // Get the collection
    val mongoClient = MongoClient(mongoClientURI.getURI)
    val collection = mongoClient.getDatabase(mongoClientURI.getDatabase).getCollection(mongoClientURI.getCollection)

    if (options.drop) {
      if (!options.quiet) Console.err.println(s"Dropping: ${mongoClientURI.getCollection}")
      Await.result(collection.drop().head(), Duration(10, TimeUnit.SECONDS))
    }

    if (!options.quiet) Console.err.print("Importing...")
    val importer = importJson(collection, importSource.getLines(), Promise[Completed]()).future
    showPinWheel(importer)
    importSource.close()
    val total = currentTime - executionStart
    if (!options.quiet) Console.err.println(s"Finished import: $total ms")
  }

  /**
   * Imports JSON into the collection
   *
   * @param collection the mongodb collection to insert into
   * @param lines the iterator from the importSource
   * @param promise the promise that is fufilled on completion or on error
   * @return the promise
   */
  private def importJson(collection: MongoCollection[Document], lines: Iterator[String], promise: Promise[Completed]): Promise[Completed] = {
    lines.hasNext match {
      case true =>
        val remainingLines = lines.take(1000)
        collection.insertMany(remainingLines.map(json => Document(json)).toSeq).subscribe(
          (completed: Completed) => remainingLines.hasNext match {
            case true => importJson(collection, remainingLines, promise)
            case false => promise.success(completed)
          },
          (failed: Throwable) => promise.failure(failed)
        )
      case false => promise.success(Completed())
    }
    promise
  }

  /**
   * Recursively convert the args list into a Map of options
   *
   * @param map - the initial option map
   * @param args - the args list
   * @return the parsed OptionMap
   */
  private def parseArgs(map: Map[String, Any], args: List[String]): Map[String, Any] = {
    args match {
      case Nil => map
      case "--quiet" :: tail =>
        parseArgs(map ++ Map("quiet" -> true), tail)
      case "-u" :: value :: tail =>
        parseArgs(map ++ Map("uri" -> value), tail)
      case "--uri" :: value :: tail =>
        parseArgs(map ++ Map("uri" -> value), tail)
      case "--file" :: value :: tail =>
        parseArgs(map ++ Map("file" -> value), tail)
      case "--drop" :: tail =>
        parseArgs(map ++ Map("drop" -> true), tail)
      case option :: tail =>
        Console.err.println("Unknown option " + option)
        Console.err.println(usage)
        sys.exit(1)
    }
  }

  /**
   * Convert the optionMap to an Options instance
   * @param optionMap the parsed args options
   * @return Options instance
   */
  private def getOptions(optionMap: Map[String, _]): Options = {
    val default = Options()
    Options(
      quiet = optionMap.getOrElse("quiet", default.quiet).asInstanceOf[Boolean],
      uri = optionMap.get("uri") match {
        case None => default.uri
        case Some(value) => Some(value.asInstanceOf[String])
      },
      file = optionMap.get("file") match {
        case None => default.file
        case Some(value) => Some(value.asInstanceOf[String])
      },
      drop = optionMap.getOrElse("drop", default.drop).asInstanceOf[Boolean]
    )
  }

  case class Options(quiet: Boolean = false, uri: Option[String] = None, file: Option[String] = None, drop: Boolean = false)

  private def currentTime = System.currentTimeMillis()

  /**
   * Shows a pinWheel in the console.err
   * @param someFuture the future we are all waiting for
   */
  private def showPinWheel(someFuture: Future[_]) {
    // Let the user know something is happening until futureOutput isCompleted
    val spinChars = List("|", "/", "-", "\\")
    while (!someFuture.isCompleted) {
      spinChars.foreach({
        case char =>
          Console.err.print(char)
          Thread sleep 200
          Console.err.print("\b")
      })
    }
    Console.err.println("")
  }

}

mongoimport.main(args)
