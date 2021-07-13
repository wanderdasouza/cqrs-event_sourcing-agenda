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

package org.mongodb.scala

import java.io.Closeable

import com.mongodb.ConnectionString
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.{MongoClients, MongoClient => JMongoClient, MongoClientSettings => LegacyMongoClientSettings}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.bson.codecs.{DocumentCodecProvider, IterableCodecProvider}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.internal.ObservableHelper.observe

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Companion object for creating new [[MongoClient]] instances
 *
 * @since 1.0
 */
object MongoClient {

  /**
   * Create a default MongoClient at localhost:27017
   *
   * @return MongoClient
   */
  def apply(): MongoClient = apply("mongodb://localhost:27017")

  /**
   * Create a MongoClient instance from a connection string uri
   *
   * @param uri the connection string
   * @return MongoClient
   */
  def apply(uri: String): MongoClient = MongoClient(uri, None)

  /**
   * Create a MongoClient instance from a connection string uri
   *
   * @param uri the connection string
   * @param mongoDriverInformation any driver information to associate with the MongoClient
   * @return MongoClient
   * @note the `mongoDriverInformation` is intended for driver and library authors to associate extra driver metadata with the connections.
   */
  def apply(uri: String, mongoDriverInformation: Option[MongoDriverInformation]): MongoClient = {
    apply(MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(uri))
      .codecRegistry(DEFAULT_CODEC_REGISTRY)
      .build(), mongoDriverInformation)
  }

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param legacyClientSettings MongoClientSettings to use for the MongoClient
   * @return MongoClient
   */
  @deprecated("Update to the supported MongoClientSettings", "2.3")
  def apply(legacyClientSettings: LegacyMongoClientSettings): MongoClient = MongoClient(legacyClientSettings, None)

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param legacyClientSettings MongoClientSettings to use for the MongoClient
   * @param mongoDriverInformation any driver information to associate with the MongoClient
   * @return MongoClient
   * @note the `mongoDriverInformation` is intended for driver and library authors to associate extra driver metadata with the connections.
   */
  @deprecated("Update to the supported MongoClientSettings", "2.3")
  def apply(legacyClientSettings: LegacyMongoClientSettings, mongoDriverInformation: Option[MongoDriverInformation]): MongoClient = {
    val builder = mongoDriverInformation match {
      case Some(info) => MongoDriverInformation.builder(info)
      case None       => MongoDriverInformation.builder()
    }
    builder.driverName(BuildInfo.name).driverVersion(BuildInfo.version).driverPlatform(s"Scala/${BuildInfo.scalaVersion}")
    MongoClient(MongoClients.create(legacyClientSettings, builder.build()))
  }

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param clientSettings MongoClientSettings to use for the MongoClient
   * @return MongoClient
   * @since 2.3
   */
  def apply(clientSettings: MongoClientSettings): MongoClient = MongoClient(clientSettings, None)

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param clientSettings MongoClientSettings to use for the MongoClient
   * @param mongoDriverInformation any driver information to associate with the MongoClient
   * @return MongoClient
   * @note the `mongoDriverInformation` is intended for driver and library authors to associate extra driver metadata with the connections.
   * @since 2.3
   */
  def apply(clientSettings: MongoClientSettings, mongoDriverInformation: Option[MongoDriverInformation]): MongoClient = {
    val builder = mongoDriverInformation match {
      case Some(info) => MongoDriverInformation.builder(info)
      case None       => MongoDriverInformation.builder()
    }
    builder.driverName(BuildInfo.name).driverVersion(BuildInfo.version).driverPlatform(s"Scala/${BuildInfo.scalaVersion}")
    MongoClient(MongoClients.create(clientSettings, builder.build()))
  }

  val DEFAULT_CODEC_REGISTRY: CodecRegistry = fromRegistries(
    fromProviders(DocumentCodecProvider(), IterableCodecProvider()),
    MongoClients.getDefaultCodecRegistry
  )
}

/**
 * A client-side representation of a MongoDB cluster.  Instances can represent either a standalone MongoDB instance, a replica set,
 * or a sharded cluster.  Instance of this class are responsible for maintaining an up-to-date state of the cluster,
 * and possibly cache resources related to this, including background threads for monitoring, and connection pools.
 *
 * Instance of this class server as factories for [[MongoDatabase]] instances.
 *
 * @param wrapped the underlying java MongoClient
 * @since 1.0
 */
case class MongoClient(private val wrapped: JMongoClient) extends Closeable {

  /**
   * Creates a client session.
   *
   * '''Note:''' A ClientSession instance can not be used concurrently in multiple asynchronous operations.
   *
   * @since 2.4
   * @note Requires MongoDB 3.6 or greater
   */
  def startSession(): SingleObservable[ClientSession] =
    observe(wrapped.startSession(_: SingleResultCallback[ClientSession]))

  /**
   * Creates a client session.
   *
   * '''Note:''' A ClientSession instance can not be used concurrently in multiple asynchronous operations.
   *
   * @param options  the options for the client session
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def startSession(options: ClientSessionOptions): SingleObservable[ClientSession] =
    observe(wrapped.startSession(options, _: SingleResultCallback[ClientSession]))

  /**
   * Gets the database with the given name.
   *
   * @param name the name of the database
   * @return the database
   */
  def getDatabase(name: String): MongoDatabase = MongoDatabase(wrapped.getDatabase(name))

  /**
   * Close the client, which will close all underlying cached resources, including, for example,
   * sockets and background monitoring threads.
   */
  def close(): Unit = wrapped.close()

  /**
   * Gets the settings that this client uses to connect to server.
   *
   * **Note**: `MongoClientSettings` is immutable.
   *
   * @return the settings
   */
  @deprecated("There is no replacement for this method", "2.3")
  lazy val settings: LegacyMongoClientSettings = wrapped.getSettings

  /**
   * Get a list of the database names
   *
   * [[http://docs.mongodb.org/manual/reference/commands/listDatabases List Databases]]
   * @return an iterable containing all the names of all the databases
   */
  def listDatabaseNames(): Observable[String] = observe(wrapped.listDatabaseNames())

  /**
   * Get a list of the database names
   *
   * [[http://docs.mongodb.org/manual/reference/commands/listDatabases List Databases]]
   *
   * @param clientSession the client session with which to associate this operation
   * @return an iterable containing all the names of all the databases
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listDatabaseNames(clientSession: ClientSession): Observable[String] = observe(wrapped.listDatabaseNames(clientSession))

  /**
   * Gets the list of databases
   *
   * @tparam TResult   the type of the class to use instead of `Document`.
   * @return the fluent list databases interface
   */
  def listDatabases[TResult]()(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): ListDatabasesObservable[TResult] =
    ListDatabasesObservable(wrapped.listDatabases(ct))

  /**
   * Gets the list of databases
   *
   * @param clientSession the client session with which to associate this operation
   * @tparam TResult the type of the class to use instead of `Document`.
   * @return the fluent list databases interface
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listDatabases[TResult](clientSession: ClientSession)(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): ListDatabasesObservable[TResult] =
    ListDatabasesObservable(wrapped.listDatabases(clientSession, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C]()(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] = ChangeStreamObservable(wrapped.watch(ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param pipeline the aggregation pipeline to apply to the change stream
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(pipeline.asJava, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](clientSession: ClientSession)(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(clientSession, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param pipeline the aggregation pipeline to apply to the change stream
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](clientSession: ClientSession, pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(clientSession, pipeline.asJava, ct))

}
