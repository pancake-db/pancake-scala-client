package com.pancakedb.client

object Exceptions {
  case class HttpException(code: Int, message: String) extends Exception(message)
  case class CorruptDataException(message: String) extends Exception(message)
  case object UnrecognizedDataTypeException extends Exception
}
