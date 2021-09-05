package br.usp.domain

import akka.actor.typed.{ActorRef, ActorSystem}
import br.usp.serialization.JsonSerializable

import scala.collection.immutable

//#user-case-classes
final case class User(name: String, tel: String)
final case class Users(users: immutable.Seq[User])
//#user-case-classes

object UserDomain {
  // actor protocol
  sealed trait Command extends JsonSerializable
  final case class GetUsers(system: ActorSystem[_], replyTo: ActorRef[Users]) extends Command
  final case class CreateUser(id: String, user: User, replyTo: ActorRef[ActionPerformed]) extends Command
  final case class GetUser(id: String, replyTo: ActorRef[GetUserResponse]) extends Command
  final case class DeleteUser(id: String, replyTo: ActorRef[ActionPerformed]) extends Command
  final case class UpdateUserName(name: String) extends Command
  final case class UpdateUserTel(tel: String) extends Command


  sealed trait Event extends JsonSerializable
  case class UserCreated(user: User) extends Event
  case class UserDeleted(id: String) extends Event
  case class UserNameUpdated(name: String) extends Event
  case class UserTelUpdated(tel: String) extends Event

  final case class GetUserResponse(maybeUser: Option[User]) extends JsonSerializable
  final case class ActionPerformed(user: User) extends JsonSerializable

}

