package br.usp

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import br.usp.domain.UserDomain._
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import br.usp.domain.User
import br.usp.serialization.JsonSerializable

object UserPersistence {

 final case class State(id: String, name: String, tel: String) extends JsonSerializable {
   def createUser(user: User) = copy(name = user.name, tel = user.tel)
    def updateName(newName: String) = copy(name = newName)
    def updateTel(newTel: String) = copy(tel = newTel)
   def removeUser = copy(name = "", tel = "")
 }

  object State {
    def empty(userId: String) = State(userId, null, null)
  }

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("User")


  private def commandHandler(context: ActorContext[Command], state: State, command: Command): ReplyEffect[Event, State] = {
    command match {
      case GetUser(replyTo) =>
        Effect
          .reply(replyTo)(GetUserResponse(Option(User(state.name, state.tel))))
      case CreateUser(user, replyTo) =>
        Effect
          .persist(UserCreated(user))
          .thenReply(replyTo)(newUserState => newUserState.id)
      case UpdateUserName(newName, replyTo) =>
        Effect
          .persist(UserNameUpdated(newName))
          .thenReply(replyTo)(newUserState =>
          GetUserResponse(Option(User(newUserState.name, newUserState.tel)))
        )
      case UpdateUserTel(newTel, replyTo) =>
        Effect
          .persist(UserTelUpdated(newTel))
          .thenReply(replyTo)(newUserState =>
          GetUserResponse(Option(User(newUserState.name, newUserState.tel)))
        )
      case DeleteUser(replyTo) =>
        Effect.persist(UserDeleted).thenReply(replyTo)(newUserState =>
          newUserState.id
        )
    }
  }

  private def eventHandler(state: State, event: Event): State = {
    event match {
      case UserCreated(user) =>
        state.createUser(user)
      case UserNameUpdated(name) =>
        state.updateName(name)
      case UserTelUpdated(tel) =>
        state.updateTel(tel)
      case UserDeleted =>
        state.removeUser
    }
  }

  def initSharding(system: ActorSystem[_]): Unit = {
    val behaviorFactory: EntityContext[Command] => Behavior[Command] = {
      entityContext =>
        UserPersistence(entityContext.entityId)
    }
    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
  }


  def apply(userId: String): Behavior[Command] = {
    Behaviors.setup { context =>
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, userId),
        emptyState = State.empty(userId),
        commandHandler = (state, command) => commandHandler(context, state, command),
        eventHandler = (state, event) => eventHandler(state, event))
    }

  }


}

