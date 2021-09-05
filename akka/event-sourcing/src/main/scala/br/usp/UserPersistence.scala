package br.usp

//#user-registry-actor


import br.usp.domain.UserDomain._
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import br.usp.domain.User
import br.usp.serialization.JsonSerializable

object UserPersistence {

 final case class State(name: String, tel: String) extends JsonSerializable {
   def createUser(user: User) = State(user.name, user.tel)
    def updateName(newName: String) = copy(name = newName)
    def updateTel(newTel: String) = copy(name = newTel)
   def removeUser = copy(null, null)
 }

  object State {
    val empty = State(null, null)
  }

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("User")


  private def commandHandler(userId: String, state: State, command: Command): ReplyEffect[Event, State] = {
    command match {
      case GetUser(id, replyTo) =>
        Effect.reply(replyTo)(GetUserResponse(Option(User(state.name, state.tel))))
      case CreateUser(id, user, replyTo) =>
        Effect.persist(UserCreated(user)).thenReply(replyTo)(newUserState => ActionPerformed(User(newUserState.name, newUserState.tel)))
//      case DeleteUser(user, replyTo) =>
//        Effect.persist(UserDeleted(user)).thenRun(_ => replyTo)
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
      case UserDeleted(_) =>
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

    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId(EntityKey.name, userId),
      emptyState = State.empty,
      commandHandler = (state, command) => commandHandler(userId, state, command),
      eventHandler = (state, event) => eventHandler(state, event))

  }


}
//#user-registry-actor

