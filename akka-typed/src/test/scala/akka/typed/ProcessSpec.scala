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
import java.util.concurrent.TimeoutException

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
        }.withTimeout(3.seconds).toBehavior
      })
    }

    def `must spawn`(): Unit = sync(runTest("spawn") {
      OpDSL[Done] { implicit opDSL ⇒
        for {
          child ← opSpawn(OpDSL[ActorRef[Done]] { implicit opDSL ⇒
            opRead.map(_ ! Done)
          }.named("child").withMailboxCapacity(2))
          self ← opProcessSelf
          _ = child ! MainCmd(self)
          msg ← opRead
        } msg should ===(Done)
      }.withTimeout(3.seconds).toBehavior
    })

    def `must watch`(): Unit = sync(runTest("watch") {
      OpDSL[Done] { implicit opDSL ⇒
        for {
          self ← opProcessSelf
          child ← opSpawn(opUnit(()).named("unit"))
          _ ← opWatch(child, Done, self)
        } opRead
      }.withTimeout(3.seconds).toBehavior
    })

    def `must unwatch`(): Unit = sync(runTest("unwatch") {
      OpDSL[String] { implicit opDSL ⇒
        for {
          self ← opProcessSelf
          child ← opSpawn(opUnit(()).named("unit"))
          cancellable ← opWatch(child, "dead", self)
          _ ← opSchedule(50.millis, "alive", self)
          msg ← { cancellable.cancel(); opRead }
        } msg should ===("alive")
      }.withTimeout(3.seconds).toBehavior
    })

    def `must respect timeouts`(): Unit = sync(runTest("timeout") {
      OpDSL[Done] { implicit opDSL ⇒
        for {
          self ← opProcessSelf
          filter = muteExpectedException[TimeoutException](occurrences = 1)
          child ← opSpawn(opRead.named("read").withTimeout(10.millis))
          _ ← opWatch(child, Done, self)
          _ ← opRead
        } filter.awaitDone(100.millis)
      }.withTimeout(3.seconds).toBehavior
    })

    def `must cancel timeouts`(): Unit = sync(runTest("timeout") {
      val childProc = OpDSL[String] { implicit opDSL ⇒
        for {
          self ← opProcessSelf
          _ ← opFork(OpDSL[String] { _ ⇒ self ! ""; opRead }.named("read").withTimeout(1.second))
        } opRead
      }.named("child").withTimeout(100.millis)

      OpDSL[Done] { implicit opDSL ⇒
        for {
          self ← opProcessSelf
          start = Deadline.now
          filter = muteExpectedException[TimeoutException](occurrences = 1)
          child ← opSpawn(childProc)
          _ ← opWatch(child, Done, self)
          _ ← opRead
        } yield {
          (Deadline.now - start) should be > 1.second
          filter.awaitDone(100.millis)
        }
      }.withTimeout(3.seconds).toBehavior
    })

    // TODO dropping messages on a subactor ref

    // TODO dropping messages on the main ref including warning when dropping Traversals (or better: make it robust)

    // TODO check that process refs are named after their processes
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
      ctx.hasEffects should ===(false)
      val procInbox = ctx.getInbox[ActorRef[Done]](procName)

      ctx.run(MainCmd(ret.ref))
      procInbox.receiveAll() should ===(List(ret.ref))

      val t = ctx.inbox.receiveMsg()
      t match {
        case sub: SubActor[_] ⇒ sub.ref.path.name should ===(procName)
        case other            ⇒ fail(s"expected SubActor, got $other")
      }
      ctx.run(t)
      ctx.getAllEffects() should ===(Nil)
      ctx.inbox.receiveAll() should ===(Nil)
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
      ctx.hasEffects should ===(false)
      val procInbox = ctx.getInbox[ActorRef[Done]](procName)

      ctx.run(MainCmd(ret.ref))
      procInbox.receiveAll() should ===(List(ret.ref))

      val t = ctx.inbox.receiveMsg()
      t match {
        case sub: SubActor[_] ⇒ sub.ref.path.name should ===(procName)
        case other            ⇒ fail(s"expected SubActor, got $other")
      }
      ctx.run(t)
      val Spawned(calledName) = ctx.getEffect()

      ctx.getAllEffects() should ===(Nil)
      ctx.inbox.receiveAll() should ===(Nil)
      ret.receiveAll() should ===(List(Done))
      ctx.isAlive should ===(false)
    }

    def `must fork`(): Unit = {
      val ret = Inbox[Done]("callRet")
      val ctx = new EffectfulActorContext("call", OpDSL[ActorRef[Done]] { implicit opDSL ⇒
        opFork(opRead.map(_ ! Done).named("forkee"))
          .map { sub ⇒
            opRead.map(sub.ref ! _)
          }
      }.named("call").toBehavior, 1, system)

      val Spawned(procName) = ctx.getEffect()
      val procInbox = ctx.getInbox[ActorRef[Done]](procName)

      val Spawned(forkName) = ctx.getEffect()
      val forkInbox = ctx.getInbox[ActorRef[Done]](forkName)
      ctx.hasEffects should ===(false)

      ctx.run(MainCmd(ret.ref))
      procInbox.receiveAll() should ===(List(ret.ref))
      ctx.getAllEffects() should ===(Nil)

      val t1 = ctx.inbox.receiveMsg()
      t1 match {
        case sub: SubActor[_] ⇒ sub.ref.path.name should ===(procName)
        case other            ⇒ fail(s"expected SubActor, got $other")
      }

      ctx.run(t1)
      forkInbox.receiveAll() should ===(List(ret.ref))
      ctx.getAllEffects() should ===(Nil)

      val t2 = ctx.inbox.receiveMsg()
      t2 match {
        case sub: SubActor[_] ⇒ sub.ref.path.name should ===(forkName)
        case other            ⇒ fail(s"expected SubActor, got $other")
      }

      ctx.run(t2)
      ctx.getAllEffects() should ===(Nil)
      ctx.inbox.receiveAll() should ===(Nil)
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
      ctx.hasEffects should ===(false)
      ctx.isAlive should ===(false)

      val Info(sys, proc, actor, value) = ret.receiveMsg()
      ret.hasMessages should ===(false)
      sys should ===(system)
      proc.path.name should ===(procName)
      actor.path should ===(proc.path.parent)
      value should ===(42)
    }

  }

  object `A ProcessDSL (adapted)` extends CommonTests with AdaptedSystem

}
