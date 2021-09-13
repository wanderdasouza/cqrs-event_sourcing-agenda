package br.usp.eventsourcing

import cats.effect.IO
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.Await
import io.finch._
import io.finch.catsEffect._
import io.finch.circe._
import io.circe.generic.auto._

import org.mongodb.scala._

import scala.collection.JavaConverters._
import scala.concurrent.duration

import util.ImplicitObservable._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._

import util.UserEventEnum

import java.util.Properties
import org.mongodb.scala.model.Filters
import org.mongodb.scala.bson.BsonString
import org.bson.BsonValue
import com.twitter.util.tunable.json
import org.mongodb.scala.bson.collection.mutable

object Main extends App {

  val mongoClient = MongoClient()

  val events = mongoClient.getDatabase("agenda").getCollection("events")

  case class User(name: String, tel: String)

  case class UserTelUpdated(tel: String)

  case class UserNameUpdated(name: String)


  def healthcheck: Endpoint[IO, String] = get(pathEmpty) {
    Ok("OK")
  }

  def createUser: Endpoint[IO, String] = post("users" :: jsonBody[User]) { u: User =>
    val userId = new ObjectId()
    val eventId = new ObjectId()
    val eventDoc = Document(
      "_id" -> eventId, 
      "event_type" -> UserEventEnum.userCreated.toString, 
      "entity_type" -> "User",
      "entity_id" -> userId,
      "event_data" -> Document(
        "name" -> u.name,
        "tel" -> u.tel
      )
    )
    events.insertOne(eventDoc).printHeadResult()
    Ok(userId.toString)
  }

  def readUser: Endpoint[IO, User] = get("users" :: path[String]) { id: String =>
    val userId = new ObjectId(id)
    val userEvents = events.find(Filters.eq("entity_id", userId)).results
    var user = User("", "")
    userEvents.foreach(e => 
      // println(e.get("event_data").get.asDocument.getString("name").getValue)
      e.getString("event_type") match {
        case "User Created" => user = User(e.get("event_data").get.asDocument.getString("name").getValue, e.get("event_data").get.asDocument.getString("tel").getValue)
        case "User Name Updated" => user = User(e.get("event_data").get.asDocument.getString("name").getValue, user.tel)
        case "User Tel Updated" => user = User(user.name, e.get("event_data").get.asDocument.getString("tel").getValue)
        case "User Deleted" => user = null
      }
    )

    if (user == null) {
      Output.empty(Status.NotFound)
    }
    else {
      Ok(user)
    }
  }

  def updateUser: Endpoint[IO, String] = patch("users" :: path[String] :: jsonBody[Map[String, String]]) { 
    (id: String, u: Map[String, String]) =>
    val userId = new ObjectId(id)
    val eventId = new ObjectId()
    var eventType = ""
    var eventData = mutable.Document()
    u.foreach((field) => field._1 match {
      case "name" => {
        eventData += "name" -> field._2
        eventType = UserEventEnum.userNameUpdated.toString
      }
      case "tel" => {
        eventData += "tel" -> field._2
         eventType = UserEventEnum.userTelUpdated.toString
      }
    })
    val eventDoc = Document(
      "_id" -> eventId, 
      "event_type" -> eventType, 
      "entity_type" -> "User",
      "entity_id" -> userId,
      "event_data" -> eventData
    )
    events.insertOne(eventDoc).printHeadResult()

    Ok(userId.toString)
  }

  def deleteUser: Endpoint[IO, String] = delete("users" :: path[String]) { 
    (id: String) => 
    val userId = new ObjectId(id)
    val eventId = new ObjectId()
    val eventDoc = Document(
      "_id" -> eventId, 
      "event_type" -> UserEventEnum.userDeleted.toString, 
      "entity_type" -> "User",
      "entity_id" -> userId,
      "event_data" -> ""
    )
    events.insertOne(eventDoc).printHeadResult()
    Ok(userId.toString)
  }

  def service: Service[Request, Response] = Bootstrap
    .serve[Text.Plain](healthcheck)
    .serve[Application.Json](createUser :+: readUser :+: updateUser :+: deleteUser)
    .toService

  Await.ready(Http.server.serve(":8082", service))
}