package br.usp.serialization

import br.usp.domain.UserDomain.ActionPerformed
import br.usp.domain.{User, UserName, UserTel, Users}

object JsonFormats {
  // import the default encoders for primitive types (Int, String, Lists etc)
  import spray.json.DefaultJsonProtocol._

  implicit val userJsonFormat = jsonFormat2(User)

  implicit val userNameJsonFormat = jsonFormat1(UserName)

  implicit val userTelJsonFormat = jsonFormat1(UserTel)

  implicit val usersJsonFormat = jsonFormat1(Users)

  implicit val actionPerformedJsonFormat = jsonFormat1(ActionPerformed)
}
