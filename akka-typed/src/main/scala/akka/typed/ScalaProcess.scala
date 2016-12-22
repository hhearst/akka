/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import scala.concurrent.duration._
import akka.actor.Cancellable

object ScalaProcess {
  import ScalaProcess._

  class RetriesExceeded extends RuntimeException

  import language.implicitConversions
  /**
   * This implicit expresses that operations that do not use their input channel can be used in any context.
   */
  private implicit def nothingIsSomething[T, U](op: Operation[Nothing, T]): Operation[U, T] = op.asInstanceOf[Operation[U, T]]

  /*
   * Terminology:
   *
   *  - a Process has a 1:1 relationship with an ActorRef
   *  - an Operation is a step that a Process takes and that produces a value
   *  - Processes are concurrent, but not distributed: all failures stop the entire Actor
   *  - each Process has its own identity (due to ActorRef), and the Actor has its own
   *    identity (an ActorRef[ActorCmd[_]]); processSelf is the Process’ identity, actorSelf is the Actor’s
   *  - timeout means failure
   *  - every Actor has a KV store for state
   *      - querying by key in TypedMultiMap (using a single element per slot)
   *      - updating is an Operation taking an Event and an implicit (Event, State) => State
   *      - persistence can then be plugged in transparently
   *      - recovery means acquiring state initially (which might trigger internal replay)
   */

  // Helper to make map behave like flatMap when needed
  trait MapAdapter[Self, Out, Mapped] {
    def lift[O](f: O ⇒ Out): O ⇒ Operation[Self, Mapped]
  }
  object MapAdapter extends MapAdapterLow {
    implicit def mapAdapterOperation[Self, M]: MapAdapter[Self, Operation[Self, M], M] =
      new MapAdapter[Self, Operation[Self, M], M] {
        override def lift[O](f: O ⇒ Operation[Self, M]): O ⇒ Operation[Self, M] = f
      }
  }
  trait MapAdapterLow {
    implicit def mapAdapterAny[Self, Out]: MapAdapter[Self, Out, Out] =
      new MapAdapter[Self, Out, Out] {
        override def lift[O](f: O ⇒ Out): O ⇒ Operation[Self, Out] = o ⇒ Return(f(o))
      }
  }

  sealed trait Operation[S, +Out] {
    def flatMap[T](f: Out ⇒ Operation[S, T]): Operation[S, T] = FlatMap(this, f)
    def map[T, Mapped](f: Out ⇒ T)(implicit ev: MapAdapter[S, T, Mapped]): Operation[S, Mapped] = flatMap(ev.lift(f))
    def foreach(f: Out ⇒ Unit): Operation[S, Unit] = flatMap(o ⇒ Return(f(o)))
    def filter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)
    def withFilter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)
  }

  private[this] case class FlatMap[S, Out1, Out2](first: Operation[S, Out1], then: Out1 ⇒ Operation[S, Out2]) extends Operation[S, Out2]
  private[this] case object ShortCircuit extends Operation[Nothing, Nothing] {
    override def flatMap[T](f: Nothing ⇒ Operation[Nothing, T]): Operation[Nothing, T] = this
  }

  // these are the private objects, to be obtained via the DSL
  private case object System extends Operation[Nothing, ActorSystem[Nothing]]
  private case object Read extends Operation[Nothing, Nothing]
  private case object Self extends Operation[Nothing, ActorRef[Any]]
  private case object ActorSelf extends Operation[Nothing, ActorRef[ActorCmd[Nothing]]]
  private case class Return[T](value: T) extends Operation[Nothing, T]
  private case class Call[S, T](process: Process[S, T]) extends Operation[Nothing, T]
  private case class Fork[S](process: Process[S, Any]) extends Operation[Nothing, SubActor[S]]
  private case class Spawn[S](process: Process[S, Any]) extends Operation[Nothing, ActorRef[ActorCmd[S]]]
  private case class Schedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T]) extends Operation[Nothing, Cancellable]

  case class Process[S, +Out](name: String, timeout: Duration, operation: Operation[S, Out])

  /*
   * The core operations: keep these minimal!
   */
  def system(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorSystem[Nothing]] = System
  def read(implicit opDSL: OpDSL): Operation[opDSL.Self, opDSL.Self] = Read
  def processSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[opDSL.Self]] = Self
  def actorSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Nothing]]] = ActorSelf
  def unit[U](value: U)(implicit opDSL: OpDSL): Operation[opDSL.Self, U] = Return(value)
  def call[Self, Out](process: Process[Self, Out])(implicit opDSL: OpDSL): Operation[opDSL.Self, Out] = Call(process)
  def nextStep[T] = OpDSL.nextStep.asInstanceOf[OpDSL.NextStep[T]]
  def fork[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[Self]] = Fork(process)
  def spawn[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Self]]] = Spawn(process)
  def schedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, Cancellable] = Schedule(delay, msg, target)

  /*
   * Convenient access to the core operations, will be even nicer with Dotty implicit function values
   */
  sealed trait OpDSL extends Any {
    type Self
  }

  object OpDSL {
    def apply[T]: Next[T] = next.asInstanceOf[Next[T]]

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
  def firstOf[T](timeout: Duration, processes: Operation[_, T]*)(implicit opDSL: OpDSL): Operation[opDSL.Self, T] = {
    def forkAll(self: ActorRef[T], index: Int = 0,
                p: List[Operation[_, T]] = processes.toList,
                acc: List[SubActor[Nothing]] = Nil)(implicit opDSL: OpDSL { type Self = T }): Operation[T, List[SubActor[Nothing]]] =
      p match {
        case Nil     ⇒ unit(acc)
        case x :: xs ⇒ fork(Process(index.toString, timeout, x)).map(sub ⇒ forkAll(self, index + 1, xs, sub :: acc))
      }
    call(Process("firstOf", timeout, OpDSL[T] { implicit opDSL ⇒
      for {
        self ← processSelf
        subs ← forkAll(self)
        value ← read
      } yield {
        subs.foreach(_.cancel())
        value
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

  def forkAndCancel[T](timeout: FiniteDuration, process: Process[T, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[T]] =
    for {
      sub ← fork(process)
      _ ← fork(Process("cancelAfter", Duration.Inf, delay(timeout, ()).foreach(_ ⇒ sub.cancel())))
    } yield unit(sub)

  def retry[S, T](timeout: FiniteDuration, retries: Int, ops: Operation[S, T])(implicit opDSL: OpDSL): Operation[opDSL.Self, T] = {
    call(Process("retry", Duration.Inf,
      firstOf(Duration.Inf, ops.map(Some(_)), delay(timeout, None))
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

  trait SubActor[-T] {
    def ref: ActorRef[T]
    def cancel(): Unit
  }
}
