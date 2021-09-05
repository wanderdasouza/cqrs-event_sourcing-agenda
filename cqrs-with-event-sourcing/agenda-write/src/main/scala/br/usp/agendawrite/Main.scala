package br.usp.agendawrite

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
import util.UserEventEnum
import org.mongodb.scala.bson.ObjectId

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.mongodb.scala.bson.collection.mutable

object Main extends App {

  case class User(name: String, tel: String)

  val mongoClient = MongoClient()

  val events = mongoClient.getDatabase("agenda").getCollection("events")

  def healthcheck: Endpoint[IO, String] = get(pathEmpty) {
    Ok("OK")
  }

  def createUser: Endpoint[IO, String] = post("users" :: jsonBody[User]) { u: User =>
    val userId = new ObjectId()
    val eventId = new ObjectId()
    val eventDoc = Document(
      "_id" -> eventId,
      "event_type" -> UserEventEnum.userCreated.toString, 
      "entity_id" -> userId,
      "event_data" -> Document(
        "name" -> u.name,
        "tel" -> u.tel
      )
    )
    writeToKafka("user-created", eventDoc.toJson())
    events.insertOne(eventDoc).printHeadResult()
    Ok(userId.toString)
  }

  def updateUser: Endpoint[IO, String] = patch("users" :: path[String] :: jsonBody[Map[String, String]]) { 
    (id: String, u: Map[String, String]) =>
    val userId = new ObjectId(id)
    u.foreach((field) => field._1 match {
      case "name" => {
        val eventDoc = Document(
          "_id" -> new ObjectId(),
          "event_type" -> UserEventEnum.userNameUpdated.toString, 
          "entity_id" -> userId,
          "event_data" -> Document(
            "name" -> field._2
          )
        )
        writeToKafka("user-name-updated", eventDoc.toJson())
        events.insertOne(eventDoc).printHeadResult()
      }
      case "tel" => {
         val eventDoc = Document(
          "_id" -> new ObjectId(),
          "event_type" -> UserEventEnum.userTelUpdated.toString, 
          "entity_id" -> userId,
          "event_data" -> Document(
            "tel" -> field._2
          )
        )
        writeToKafka("user-tel-updated", eventDoc.toJson())
        events.insertOne(eventDoc).printHeadResult()
      }
    })
    
    Ok(userId.toString)
  }

  def deleteUser: Endpoint[IO, String] = delete("users" :: path[String]) { 
    (id: String) => 
    val userId = new ObjectId(id)
    val eventId = new ObjectId()
    val eventDoc = Document(
      "_id" -> eventId, 
      "event_type" -> UserEventEnum.userDeleted.toString,
      "entity_id" -> userId,
      "event_data" -> ""
    )
    writeToKafka("user-deleted", eventDoc.toJson())
    events.insertOne(eventDoc).printHeadResult()
    Ok(userId.toString)
  }

  def writeToKafka(topic: String, userCreatedJSON: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9094")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, "key", userCreatedJSON)
    producer.send(record)
    producer.close()
  }

  def service: Service[Request, Response] = Bootstrap
    .serve[Text.Plain](healthcheck)
    .serve[Application.Json](createUser :+: updateUser :+: deleteUser)
    .toService

  Await.ready(Http.server.serve(":8081", service))
}

