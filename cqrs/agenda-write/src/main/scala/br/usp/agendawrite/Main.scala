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
import org.mongodb.scala.bson.ObjectId

import java.util.Properties
import org.apache.kafka.clients.producer._

object Main extends App {

  case class User(name: String, tel: String)

  val mongoClient = MongoClient()

  val users = mongoClient.getDatabase("agenda").getCollection("users")

  def healthcheck: Endpoint[IO, String] = get(pathEmpty) {
    Ok("OK")
  }


  def createUser: Endpoint[IO, String] = post("users" :: jsonBody[User]) { u: User =>
    val id = new ObjectId()
    val userDoc = Document("_id" -> id, "name" -> u.name, "tel" -> u.tel)
    val query = users.insertOne(userDoc).printHeadResult()
    writeToKafka("users", userDoc.toJson())
    Ok(id.toString())
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
    .serve[Application.Json](createUser)
    .toService

  Await.ready(Http.server.serve(":8081", service))
}

