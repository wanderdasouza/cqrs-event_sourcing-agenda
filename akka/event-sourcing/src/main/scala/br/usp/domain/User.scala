package br.usp.domain

import akka.actor.typed.{ActorRef, ActorSystem}
import br.usp.serialization.JsonSerializable

import scala.collection.immutable

//#user-case-classes
final case class User(name: String, tel: String)
final case class Users(users: immutable.Seq[User])

final case class UserName(name: String)
final case class UserTel(tel: String)

object UserDomain {
  // actor protocol
  sealed trait Command extends JsonSerializable
  final case class CreateUser(user: User, replyTo: ActorRef[ActionPerformed]) extends Command
  final case class GetUser(replyTo: ActorRef[GetUserResponse]) extends Command
  final case class DeleteUser(replyTo: ActorRef[ActionPerformed]) extends Command
  final case class UpdateUserName(name: String, replyTo: ActorRef[GetUserResponse]) extends Command
  final case class UpdateUserTel(tel: String, replyTo: ActorRef[GetUserResponse]) extends Command


  sealed trait Event extends JsonSerializable
  case class UserCreated(user: User) extends Event
  case class UserDeleted(id: String) extends Event
  case class UserNameUpdated(name: String) extends Event
  case class UserTelUpdated(tel: String) extends Event

  final case class GetUserResponse(maybeUser: Option[User]) extends JsonSerializable
  final case class ActionPerformed(user: User) extends JsonSerializable

}

