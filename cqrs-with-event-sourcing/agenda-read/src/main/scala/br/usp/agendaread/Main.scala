package br.usp.agendaread

import cats.effect.IO
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Await
import io.finch._
import io.finch.catsEffect._
import io.finch.circe._
import io.circe.generic.auto._
import org.mongodb.scala._

import org.mongodb.scala.ObservableImplicits

import scala.collection.JavaConverters._
import scala.concurrent.duration

import util.ImplicitObservable._
import org.mongodb.scala.bson.ObjectId


import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

import java.util.Arrays
import java.util.concurrent.Executors
import scala.util.parsing.json.JSON
import scala.collection.mutable
import com.twitter.util.tunable.json
import com.twitter.util.tunable.json
import scala.util.parsing.json.JSONObject
import org.bson.BSONObject
import org.mongodb.scala.bson.BsonDocument
import java.util.Collection


object Main extends App {
  
  case class User(name: String, tel: String)

  val mongoClient = MongoClient("mongodb://localhost:3001")

  val users = mongoClient.getDatabase("agenda").getCollection("users")

  def healthcheck: Endpoint[IO, String] = get(pathEmpty) {
    Ok("OK")
  }


  def allUsers: Endpoint[IO, List[User]] = get("users") { 
    val query = users.find.results
    val listUsers: List[User] = query.map(q => User(q.getString("name"), q.getString("tel"))).toList

    Ok(listUsers)
  }

  def consumeFromKafka(topics: Collection[String]) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9094")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(topics) 
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000).asScala

          for (record <- records) {
            val b = BsonDocument(record.value())
            val eventDoc = Document(b) 
            eventDoc.getString("event_type") match {
              case "User Created" => {
                users.insertOne(Document(
                  "_id" -> eventDoc.getObjectId("entity_id"), 
                  "name" -> eventDoc.get("event_data").get.asDocument.getString("name").getValue,
                  "tel" -> eventDoc.get("event_data").get.asDocument.getString("tel").getValue
                  )
                ).printResults()
              }

              case "User Deleted" => {
                users.deleteOne(Document("_id" -> eventDoc.getObjectId("entity_id"))).printResults()
              }

              case "User Tel Updated" => {
                users.updateOne(
                  Document("_id" -> eventDoc.getObjectId("entity_id")), 
                  Document("$set" -> Document(
                    "tel" -> eventDoc.get("event_data").get.asDocument.getString("tel").getValue)
                  )
                ).printResults()
              }

              case "User Name Updated" => {
                users.updateOne(
                  Document("_id" -> eventDoc.getObjectId("entity_id")), 
                  Document("$set" -> Document(
                    "name" -> eventDoc.get("event_data").get.asDocument.getString("name").getValue)
                  )
                ).printResults()
              }
            } 
            // users.insertOne(userDoc).results()
            println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
    
  }

  val topics = Array("user-created", "user-name-updated", "user-tel-updated", "user-deleted").toList.asJava
  consumeFromKafka(topics)

  def service: Service[Request, Response] = Bootstrap
    .serve[Text.Plain](healthcheck)
    .serve[Application.Json](allUsers)
    .toService
  
  Await.ready(Http.server.serve(":8082", service))

}