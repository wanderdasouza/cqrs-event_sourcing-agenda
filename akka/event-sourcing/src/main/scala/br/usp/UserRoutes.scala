package br.usp

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route

import scala.concurrent.{Await, Future}
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.contrib.persistence.mongodb.MongoReadJournal
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import br.usp.domain.{User, UserName, UserTel, Users}
import br.usp.domain.UserDomain._
import org.bson.types.ObjectId

import scala.concurrent.duration.DurationInt


class UserRoutes(implicit val system: ActorSystem[_]) {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import br.usp.serialization.JsonFormats._
  //#import-json-formats

  private val sharding = ClusterSharding(system)

  // If ask takes more time than this to complete the request is failed
  private implicit val timeout = Timeout.create(system.settings.config.getDuration("my-app.routes.ask-timeout"))

  def getUsers = {
    val readJournal =
      PersistenceQuery(system).readJournalFor[CurrentPersistenceIdsQuery](MongoReadJournal.Identifier)
    readJournal
      .currentPersistenceIds()
      .map(id => Await.result(getUser(id.split("\\|").last), 4.second).maybeUser.get)
      .runFold(Set.empty[User])((set, user) => set + user)
  }
  def updateUserName(userId: String, newName: String): Future[GetUserResponse] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(UpdateUserName(newName, _))
  }
  def updateUserTel(userId: String, newTel: String): Future[GetUserResponse] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(UpdateUserTel(newTel, _))
  }
  def getUser(userId: String): Future[GetUserResponse] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(GetUser(_))
  }
  def createUser(user: User): Future[ActionPerformed] = {
    val id = new ObjectId().toString
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, id)
    entityRef.ask(CreateUser(user, _))
  }
  def deleteUser(userId: String): Future[ActionPerformed] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(DeleteUser( _))
  }

  val userRoutes: Route =
    pathPrefix("users") {
      concat(
        pathEnd {
          concat(
            get {
              onSuccess(getUsers) { users =>
                complete(Users(users.toSeq))
              }
            },
            post {
              entity(as[User]) { user =>
                onSuccess(createUser(user)) { performed =>
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        },
        path(Segment) { id =>
          concat(
            get {
              rejectEmptyResponse {
                onSuccess(getUser(id)) { response =>
                  complete(response.maybeUser)
                }
              }
            },
            patch {
              rejectEmptyResponse {
                concat(
                  entity(as[UserTel]) { newEditRequest =>
                    onSuccess(updateUserTel(id, newEditRequest.tel)) { response =>
                      complete(response.maybeUser)
                    }
                  },
                  entity(as[UserName]) { newEditRequest =>
                    onSuccess(updateUserName(id, newEditRequest.name)) { response =>
                      complete(response.maybeUser)
                    }
                  }
                )
              }
            },
            delete {
              onSuccess(deleteUser(id)) { performed =>
                complete((StatusCodes.OK, performed))
              }
            }
          )
        })
    }
}
