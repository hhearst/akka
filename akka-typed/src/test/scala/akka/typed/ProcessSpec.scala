/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import ScalaProcess._
import patterns.Receptionist._
import scala.concurrent.duration._
import AskPattern._

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

      def register[T](server: ActorRef[T], key: ServiceKey[T]) =
        OpDSL[Registered[T]] { implicit opDSL ⇒
          for {
            self ← processSelf
            system ← system
          } yield {
            val r = system.receptionist
            r ! Register(key, server)(self)
            read
          }
        }

      def backend =
        OpDSL[Login] { implicit opDSL ⇒
          for {
            self ← processSelf
            _ ← call(register(self, LoginService).named("registerBackend"))
            store ← fork(backendStore.named("store"))
          } yield backendLoop(store.ref)
        }

      def backendLoop(store: ActorRef[Store]): Operation[Login, Nothing] =
        OpDSL[Login] { implicit opDSL ⇒
          for (Login(replyTo) ← read) yield {
            replyTo ! AuthSuccess(store)
            backendLoop(store)
          }
        }

      lazy val backendStore: Operation[Store, Nothing] =
        OpDSL[Store] { implicit opDSL ⇒
          for (GetData(replyTo) ← read) yield {
            replyTo ! DataResult("yeehah")
            backendStore
          }
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
            _ ← unit({ backend ! Login(self) })
            AuthSuccess(store) ← read
            data ← nextStep[DataResult](1, { implicit opDSL ⇒
              for {
                self ← processSelf
                _ = store ! GetData(self)
              } yield read
            })
          } req.replyTo ! Response(data.msg)
        }

      def loop(backend: ActorRef[Login]): Operation[Request, Unit] =
        OpDSL[Request] { implicit opDSL ⇒
          for {
            req ← read
            _ ← forkAndCancel(5.seconds, talkWithBackend(backend, req).named("worker"))
          } yield loop(backend)
        }

      def server =
        OpDSL[Request] { implicit op ⇒
          for {
            _ ← spawn(backend.named("backend"))
            self ← processSelf
            _ ← retry(1.second, 3, register(self, RequestService).named("register"))
            backend ← retry(1.second, 3, getBackend.named("getBackend"))
          } yield loop(backend.addresses.head)
        }

      val sys = ActorSystem("op", toBehavior(server.named("server")))
      try {
        val f = sys ? ((r: ActorRef[Response]) ⇒ MainCmd(Request("hello", r)))
        f.futureValue should ===(Response("yeehah"))
      } finally {
        sys.terminate()
      }
    }

  }

}
