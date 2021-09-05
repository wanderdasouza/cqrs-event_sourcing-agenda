package br.usp

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route

import scala.concurrent.Future
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import br.usp.domain.User
import br.usp.domain.UserDomain._
import org.bson.types.ObjectId
import org.slf4j.Logger

import java.lang.System.Logger

//#import-json-formats
//#user-routes-class
class UserRoutes(implicit val system: ActorSystem[_]) {

  //#user-routes-class
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import br.usp.serialization.JsonFormats._
  //#import-json-formats

  private val sharding = ClusterSharding(system)

  // If ask takes more time than this to complete the request is failed
  private implicit val timeout = Timeout.create(system.settings.config.getDuration("my-app.routes.ask-timeout"))


//  def getUsers: Future[Users] = {
//    //userRegistry.ask(GetUsers(system,_))
//  }
  def getUser(userId: String): Future[GetUserResponse] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(GetUser(userId, _))
  }
  def createUser(user: User): Future[ActionPerformed] = {
    val id = new ObjectId().toString
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, id)
    entityRef.ask(CreateUser(id, user, _))
  }

  def deleteUser(userId: String): Future[ActionPerformed] = {
    val entityRef = sharding.entityRefFor(UserPersistence.EntityKey, userId)
    entityRef.ask(DeleteUser(userId, _))
  }

  //#all-routes
  //#users-get-post
  //#users-get-delete
  val userRoutes: Route =
    pathPrefix("users") {
      concat(
        //#users-get-delete
        pathEnd {
          concat(
            get {
              //complete(getUsers())

              complete((StatusCodes.OK))
            },
            post {
              entity(as[User]) { user =>
                onSuccess(createUser(user)) { performed =>
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        },
        //#users-get-delete
        //#users-get-post
        path(Segment) { id =>
          concat(
            get {
              //#retrieve-user-info
              rejectEmptyResponse {
                onSuccess(getUser(id)) { response =>
                  complete(response.maybeUser)
                }
              }
              //#retrieve-user-info
            },
            delete {
              //#users-delete-logic
              onSuccess(deleteUser(id)) { performed =>
                complete((StatusCodes.OK, performed))
              }
              //#users-delete-logic
            })
        })
      //#users-get-delete
    }
  //#all-routes
}
