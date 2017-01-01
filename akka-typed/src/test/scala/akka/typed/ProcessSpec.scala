/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import ScalaProcess._
import patterns.Receptionist._
import scala.concurrent.duration._
import AskPattern._
import org.scalatest.Succeeded
import akka.actor.InvalidActorNameException
import akka.Done
import Effect._

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

  trait CommonTests {
    implicit def system: ActorSystem[TypedSpec.Command]

    def `demonstrates working processes`(): Unit = {

      def register[T](server: ActorRef[T], key: ServiceKey[T]) =
        OpDSL[Registered[T]] { implicit opDSL ⇒
          for {
            self ← opProcessSelf
            sys ← opSystem
          } yield {
            sys.receptionist ! Register(key, server)(self)
            opRead
          }
        }

      val backendStore =
        OpDSL.loopInf[Store] { implicit opDSL ⇒
          for (GetData(replyTo) ← opRead) yield {
            replyTo ! DataResult("yeehah")
          }
        }

      val backend =
        OpDSL[Login] { implicit opDSL ⇒
          for {
            self ← opProcessSelf
            _ ← opCall(register(self, LoginService).named("registerBackend"))
            store ← opFork(backendStore.named("store"))
          } yield OpDSL.loopInf { _ ⇒
            for (Login(replyTo) ← opRead) yield {
              replyTo ! AuthSuccess(store.ref)
            }
          }
        }

      val getBackend =
        OpDSL[Listing[Login]] { implicit opDSL ⇒
          for {
            self ← opProcessSelf
            system ← opSystem
            _ = system.receptionist ! Find(LoginService)(self)
          } yield opRead
        }

      def talkWithBackend(backend: ActorRef[Login], req: Request) =
        OpDSL[AuthResult] { implicit opDSL ⇒
          for {
            self ← opProcessSelf
            _ ← opUnit({ backend ! Login(self) })
            AuthSuccess(store) ← opRead
            data ← opNextStep[DataResult](1, { implicit opDSL ⇒
              for {
                self ← opProcessSelf
                _ = store ! GetData(self)
              } yield opRead
            })
          } req.replyTo ! Response(data.msg)
        }

      val server =
        OpDSL[Request] { implicit op ⇒
          for {
            _ ← opSpawn(backend.named("backend"))
            self ← opProcessSelf
            _ ← retry(1.second, 3, register(self, RequestService).named("register"))
            backend ← retry(1.second, 3, getBackend.named("getBackend"))
          } yield OpDSL.loopInf { _ ⇒
            for (req ← opRead)
              forkAndCancel(5.seconds, talkWithBackend(backend.addresses.head, req).named("worker"))
          }
        }

      sync(runTest("complexOperations") {
        OpDSL[Response] { implicit opDSL ⇒
          for {
            serverRef ← opSpawn(server.named("server").withMailboxCapacity(20))
            self ← opProcessSelf
          } yield OpDSL.loop(2) { _ ⇒
            for {
              _ ← opUnit(serverRef ! MainCmd(Request("hello", self)))
              msg ← opRead
            } msg should ===(Response("yeehah"))
          }.map { results ⇒
            results should ===(List(Succeeded, Succeeded))
          }
        }.named("main").toBehavior
      })
    }
  }

  object `A ProcessDSL (native)` extends CommonTests with NativeSystem {

    def `must reject invalid process names early`(): Unit = {
      a[InvalidActorNameException] mustBe thrownBy {
        opRead(null).named("$hello")
      }
      a[InvalidActorNameException] mustBe thrownBy {
        opRead(null).named("hello").copy(name = "$hello")
      }
      a[InvalidActorNameException] mustBe thrownBy {
        Process("$hello", Duration.Inf, 1, null)
      }
    }

    def `must read`(): Unit = {
      val ret = Inbox[Done]("readRet")
      val ctx = new EffectfulActorContext("read", OpDSL[ActorRef[Done]] { implicit opDSL ⇒
        opRead.foreach(_ ! Done)
      }.named("read").toBehavior, 1, system)

      val Spawned(procName) = ctx.getEffect()
      val Watched(procRef) = ctx.getEffect()
      procRef.path.name should ===(procName)
      ctx.hasEffects should ===(false)
      val procInbox = ctx.getInbox[ActorRef[Done]](procName)

      ctx.run(MainCmd(ret.ref))
      procInbox.receiveAll() should ===(List(ret.ref))

      val t: ActorCmd[ActorRef[Done]] = ctx.inbox.receiveMsg() match {
        case sub: SubActor[_] ⇒
          sub.ref should ===(procRef)
          sub
        case other ⇒ fail(s"expected SubActor, got $other")
      }
      ctx.run(t)
      ctx.getAllEffects() should ===(Nil)
      ret.receiveAll() should ===(List(Done))
      ctx.isAlive should ===(false)
    }

    def `must call`(): Unit = {
      val ret = Inbox[Done]("callRet")
      val ctx = new EffectfulActorContext("call", OpDSL[ActorRef[Done]] { implicit opDSL ⇒
        opRead.flatMap(replyTo ⇒ opCall(OpDSL[String] { implicit opDSL ⇒
          opUnit(replyTo ! Done)
        }.named("called")))
      }.named("call").toBehavior, 1, system)

      val Spawned(procName) = ctx.getEffect()
      val Watched(procRef) = ctx.getEffect()
      procRef.path.name should ===(procName)
      ctx.hasEffects should ===(false)
      val procInbox = ctx.getInbox[ActorRef[Done]](procName)

      ctx.run(MainCmd(ret.ref))
      procInbox.receiveAll() should ===(List(ret.ref))

      val t: ActorCmd[ActorRef[Done]] = ctx.inbox.receiveMsg() match {
        case sub: SubActor[_] ⇒
          sub.ref should ===(procRef)
          sub
        case other ⇒ fail(s"expected SubActor, got $other")
      }
      ctx.run(t)
      val Spawned(calledName) = ctx.getEffect()
      val Watched(calledRef) = ctx.getEffect()
      calledRef.path.name should ===(calledName)

      ctx.getAllEffects() should ===(Nil)
      ret.receiveAll() should ===(List(Done))
      ctx.isAlive should ===(false)
    }

    def `must return all the things`(): Unit = {
      case class Info(sys: ActorSystem[Nothing], proc: ActorRef[Nothing], actor: ActorRef[Nothing], value: Int)
      val ret = Inbox[Info]("thingsRet")
      val ctx = new EffectfulActorContext("things", OpDSL[ActorRef[Done]] { implicit opDSL ⇒
        for {
          sys ← opSystem
          proc ← opProcessSelf
          actor ← opActorSelf
          value ← opUnit(42)
        } ret.ref ! Info(sys, proc, actor, value)
      }.named("things").toBehavior, 1, system)

      val Spawned(procName) = ctx.getEffect()
      val Watched(procRef) = ctx.getEffect()
      procRef.path.name should ===(procName)
      ctx.hasEffects should ===(false)
      ctx.isAlive should ===(false)

      val Info(sys, proc, actor, value) = ret.receiveMsg()
      ret.hasMessages should ===(false)
      sys should ===(system)
      proc should ===(procRef)
      actor.path should ===(proc.path.parent)
      value should ===(42)
    }

  }

  object `A ProcessDSL (adapted)` extends CommonTests with AdaptedSystem

}
