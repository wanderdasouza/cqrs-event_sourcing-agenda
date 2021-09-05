package br.usp.agendawrite.util

object UserEventEnum extends Enumeration 
{
    type UserEventEnum = Value
      
    // Assigning values 
    val userCreated = Value("User Created")
    val userTelUpdated = Value("User Tel Updated")
    val userNameUpdated = Value("User Name Updated")
    val userDeleted = Value("User Deleted")
}