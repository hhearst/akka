/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import ScalaProcess._
import patterns.Receptionist._
import scala.concurrent.duration._

object ProcessSpec {

  sealed abstract class RequestService extends ServiceKey[Request]
  object RequestService extends RequestService

  case class Request(req: String, replyTo: ActorRef[Response])
  case class Response(res: String)

  sealed abstract class LoginService extends ServiceKey[Login]
  object LoginService extends LoginService

  case class Login(replyTo: ActorRef[AuthResult])
  sealed trait AuthResult
  case object AuthRejected extends AuthResult
  case class AuthSuccess(next: ActorRef[Store]) extends AuthResult

  sealed trait Store
  case class GetData(replyTo: ActorRef[DataResult]) extends Store
  case class DataResult(msg: String)
}

class ProcessSpec extends TypedSpec {
  import ProcessSpec._

  object `A Process DSL` {

    def `must create working processes`(): Unit = {
      def register(server: ActorRef[Request]) =
        OpDSL[Registered[Request]] { implicit op ⇒
          for {
            self ← processSelf
            system ← system
            _ = system.receptionist ! Register(RequestService, server)(self)
          } yield read
        }

      def getBackend =
        OpDSL[Listing[Login]] { implicit opDSL ⇒
          for {
            self ← processSelf
            system ← system
            _ = system.receptionist ! Find(LoginService)(self)
          } yield read
        }

      def talkWithBackend(backend: ActorRef[Login], req: Request) =
        OpDSL[AuthResult] { implicit opDSL ⇒
          for {
            self ← processSelf
            _ ← unit(backend ! Login(self))
            AuthSuccess(store) ← read
            data ← nextStep[DataResult] { implicit opDSL ⇒
              for {
                self ← processSelf
                _ = store ! GetData(self)
              } yield read
            }
          } req.replyTo ! Response(data.msg)
        }

      def loop(backend: ActorRef[Login]): Operation[Request, Unit] =
        OpDSL[Request] { implicit opDSL ⇒
          for {
            req ← read
            _ ← forkAndCancel(5.seconds, Process("worker", 10.seconds, talkWithBackend(backend, req)))
          } yield loop(backend)
        }

      def server =
        OpDSL[Request] { implicit op ⇒
          for {
            self ← processSelf
            _ ← retry(1.second, 3, register(self))
            backend ← retry(1.second, 3, getBackend)
          } yield loop(backend.addresses.head)
        }

      val sys = ActorSystem("op", toBehavior(server))
      sys ! MainCmd(Request("hello", null))
    }

  }

}
