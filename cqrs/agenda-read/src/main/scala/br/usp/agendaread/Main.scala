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


object Main extends App {

  
  case class User(name: String, tel: String)

  val mongoClient = MongoClient("mongodb://localhost:3001")

  val users = mongoClient.getDatabase("agenda").getCollection("users")

  def healthcheck: Endpoint[IO, String] = get(pathEmpty) {
    Ok("OK")
  }


  def allUsers: Endpoint[IO, String] = get("users") { 
    val query = users.find.printResults()
    Ok("Finalizado")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9094")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(Arrays.asList(topic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000).asScala

          for (record <- records) {
            val b = BsonDocument(record.value())
            val userDoc = Document(b) 
            println(userDoc)
            users.insertOne(userDoc).results()
            println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
    
  }


  consumeFromKafka("users")

  def service: Service[Request, Response] = Bootstrap
    .serve[Text.Plain](healthcheck)
    .serve[Application.Json](allUsers)
    .toService
  
  Await.ready(Http.server.serve(":8082", service))

}