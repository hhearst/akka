/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import scala.concurrent.duration._
import akka.actor.Cancellable

private[typed] trait ScalaProcess {
  import ScalaProcess._

  /*
   * Terminology:
   *
   *  - a Process has a 1:1 relationship with an ActorRef
   *  - an Operation is a step that a Process takes and that produces a value
   *  - Processes are concurrent, but not distributed: all failures stop the entire Actor
   *  - each Process has its own identity (due to ActorRef), and the Actor has its own
   *    identity (an ActorRef[ActorCmd[_]]); Self is the Process’ identity, ActorSelf is the Actor’s
   *  - timeout means failure
   */

  // Helper to make map behave like flatMap when needed
  trait MapAdapter[Out, Mapped] {
    def lift[O, Self](f: O ⇒ Out): O ⇒ Operation[Self, Mapped]
  }
  object MapAdapter extends MapAdapterLow {
    implicit def mapAdapterOperation[S, M]: MapAdapter[Operation[S, M], M] =
      new MapAdapter[Operation[S, M], M] {
        override def lift[O, Self](f: O ⇒ Operation[S, M]): O ⇒ Operation[Self, M] = f.asInstanceOf[O => Operation[Self, M]]
      }
  }
  trait MapAdapterLow {
    implicit def mapAdapterAny[Out]: MapAdapter[Out, Out] =
      new MapAdapter[Out, Out] {
        override def lift[O, Self](f: O ⇒ Out): O ⇒ Operation[Self, Out] = o ⇒ Return(f(o))
      }
  }

  def res[T](value: T): Operation[T, T] = OpDSL[T] { implicit opDSL ⇒
    for {
      self ← processSelf
      _ ← schedule(null, value, self)
    } yield read
  }

  sealed trait Operation[-S, +Out] {
    def flatMap[U <: S, T](f: Out ⇒ Operation[U, T]): Operation[U, T] = FlatMap(this, f)
    def map[U <: S, T, Mapped](f: Out ⇒ T)(implicit ev: MapAdapter[T, Mapped]): Operation[U, Mapped] = flatMap(ev.lift(f))
    def foreach(f: Out ⇒ Unit): Operation[S, Unit] = flatMap(o ⇒ Return(f(o)))
    def filter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)
    def withFilter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)
  }

  private[this] case class FlatMap[S, Out1, Out2](first: Operation[S, Out1], then: Out1 ⇒ Operation[S, Out2]) extends Operation[S, Out2]
  private[this] case object ShortCircuit extends Operation[Any, Nothing] {
    override def flatMap[U, T](f: Nothing ⇒ Operation[U, T]): ShortCircuit.type = this
  }

  // these are the private objects, to be obtained via the DSL
  private case object System extends Operation[Any, ActorSystem[Nothing]]
  private case object Read extends Operation[Any, Nothing]
  private case object Self extends Operation[Any, ActorRef[Any]]
  private case object ActorSelf extends Operation[Any, ActorRef[ActorCmd[Nothing]]]
  private case class Return[T](value: T) extends Operation[Any, T]
  private case class Call[T](process: Process[Nothing, T]) extends Operation[Any, T]
  private case class Fork[S](process: Process[S, Any]) extends Operation[Any, SubActor[S]]
  private case class Spawn[S](process: Process[S, Any]) extends Operation[Any, ActorRef[ActorCmd[S]]]
  private case class Schedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T]) extends Operation[Any, Cancellable]

  case class Process[-S, +Out](name: String, timeout: Duration, operation: Operation[S, Out])
  object ProcessN {
    def apply[Out](name: String, timeout: Duration, operation: Operation[Nothing, Out]): Process[Nothing, Out] =
      Process[Nothing, Out](name, timeout, operation)
  }

  /*
   * The core operations: keep these minimal!
   */
  def system(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorSystem[Nothing]] = System
  def read(implicit opDSL: OpDSL): Operation[opDSL.Self, opDSL.Self] = Read
  def processSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[opDSL.Self]] = Self
  def actorSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Nothing]]] = ActorSelf
  def unit[U](value: U)(implicit opDSL: OpDSL): Operation[opDSL.Self, U] = Return(value)
  def call[Out](process: Process[Nothing, Out])(implicit opDSL: OpDSL): Operation[opDSL.Self, Out] = Call(process)
  def nextStep[T] = OpDSL.nextStep.asInstanceOf[OpDSL.NextStep[T]]
  def fork[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[Self]] = Fork(process)
  def forkN(process: Process[Nothing, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[Nothing]] = Fork[Nothing](process)
  def spawn[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Self]]] = Spawn(process)
  def spawnN(process: Process[Nothing, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Nothing]]] = Spawn[Nothing](process)
  def schedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, Cancellable] = Schedule(delay, msg, target)

  /*
   * Convenient access to the core operations, will be even nicer with Dotty implicit function values
   */
  sealed trait OpDSL extends Any {
    type Self
  }

  object OpDSL {
    def apply[T]: Next[T] = next.asInstanceOf[Next[T]]

    /*
     * This implicit is picked up if nothing else is available. It is unproblematic
     * because the resulting input operations are unconstrained and therefore fit
     * into any context.
     */
    implicit object noReads extends OpDSL {
      type Self = Any
    }

    trait Next[T] {
      def apply[U](body: OpDSL { type Self = T } ⇒ Operation[T, U]): Operation[T, U] = body(null)
    }
    private object next extends Next[Nothing]

    trait NextStep[T] {
      def apply[U](body: OpDSL { type Self = T } ⇒ Operation[T, U])(implicit opDSL: OpDSL): Operation[opDSL.Self, U] =
        Call(Process("nextStep", Duration.Inf, body(null)))
    }
    private[typed] object nextStep extends NextStep[Nothing]
  }

  /*
   * Derived operations
   */
  def firstOf[T](timeout: Duration, processes: Operation[Nothing, T]*): Operation[Any, T] = {
    def forkAll(self: ActorRef[T], index: Int = 0,
                p: List[Operation[Nothing, T]] = processes.toList,
                acc: List[SubActor[Nothing]] = Nil)(implicit opDSL: OpDSL { type Self = T }): Operation[T, List[SubActor[Nothing]]] =
      p match {
        case Nil     ⇒ unit(acc)
        case x :: xs ⇒ forkN(ProcessN(index.toString, timeout, x)).map(sub ⇒ forkAll(self, index + 1, xs, sub :: acc))
      }
    call(ProcessN("firstOf", timeout, OpDSL[T] { implicit opDSL ⇒
      for {
        self ← processSelf
        subs ← forkAll(self)
        value ← read
      } yield {
        subs.foreach(_.cancel())
        unit(value)
      }
    }))
  }

  def delay[T](time: FiniteDuration, value: T): Operation[T, T] =
    OpDSL[T] { implicit opDSL ⇒
      for {
        self ← processSelf
        _ ← schedule(time, value, self)
      } yield read
    }

  def forkAndCancel[T](timeout: FiniteDuration, process: Process[T, Any]): Operation[Any, SubActor[T]] =
    for {
      sub ← fork(process)
      _ ← fork(Process("cancelAfter", Duration.Inf, delay(timeout, ()).foreach(_ ⇒ sub.cancel())))
    } yield unit(sub)

  def retry[T](timeout: FiniteDuration, retries: Int, ops: Operation[Nothing, T]): Operation[Any, T] = {
    call(ProcessN("retry", Duration.Inf,
      firstOf(Duration.Inf, ops.map(res ⇒ unit(Some(res))), delay(timeout, None))
    ))
      .map {
        case Some(res)           ⇒ unit(res)
        case None if retries > 0 ⇒ retry(timeout, retries - 1, ops)
        case None                ⇒ throw new RetriesExceeded
      }
  }

  /*
   * Convert it to runnable Behavior.
   */
  def toBehavior[S, T](op: Operation[S, T]): Behavior[ActorCmd[T]] = ???

  sealed trait ActorCmd[+T]
  case class MainCmd[T](cmd: T) extends ActorCmd[T]

  trait SubActor[T] {
    def ref: ActorRef[T]
    def cancel(): Unit
  }
}

object ScalaProcess extends ScalaProcess {
  // so that this high-level DSL can be imported separately

  class RetriesExceeded extends RuntimeException
}
