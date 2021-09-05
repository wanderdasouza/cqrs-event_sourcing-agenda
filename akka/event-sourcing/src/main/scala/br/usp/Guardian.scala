package br.usp

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object Guardian {

  def apply(httpPort: Int): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
    UserPersistence.initSharding(context.system)

    val routes = new UserRoutes()(context.system)
    HttpServer.startHttpServer(routes.userRoutes, httpPort)(context.system)

    Behaviors.empty
  }
}
