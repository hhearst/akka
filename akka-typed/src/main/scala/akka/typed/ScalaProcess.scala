/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed

import scala.concurrent.duration._
import akka.{ actor ⇒ a }

object ScalaProcess {
  import ScalaProcess._

  class RetriesExceeded extends RuntimeException

  import language.implicitConversions
  /**
   * This implicit expresses that operations that do not use their input channel can be used in any context.
   */
  private implicit def nothingIsSomething[T, U](op: Operation[Nothing, T]): Operation[U, T] = op.asInstanceOf[Operation[U, T]]

  /*
   * Convenient access to the core operations, will be even nicer with Dotty implicit function values
   */
  sealed trait OpDSL extends Any {
    type Self
  }

  object OpDSL {
    private val _unit: Operation[Nothing, Null] = opUnit(null)(null: OpDSL { type Self = Nothing })
    private def unit[S, Out]: Operation[S, Out] = _unit.asInstanceOf[Operation[S, Out]]

    def loopInf[S]: NextLoopInf[S] = nextLoopInf.asInstanceOf[NextLoopInf[S]]
    trait NextLoopInf[S] {
      def apply[U](body: OpDSL { type Self = S } ⇒ Operation[S, U]): Operation[S, Nothing] = {
        lazy val l: Operation[S, Nothing] = unit[S, OpDSL { type Self = S }].flatMap(body).flatMap(_ ⇒ l)
        l
      }
    }
    private object nextLoopInf extends NextLoopInf[Nothing]

    def loop[S]: NextLoop[S] = nextLoop.asInstanceOf[NextLoop[S]]
    trait NextLoop[S] {
      def apply[Out](n: Int)(body: OpDSL { type Self = S } ⇒ Operation[S, Out]): Operation[S, List[Out]] = {
        require(n > 0, "number of iterations must be positive")
        def step(n: Int, acc: List[Out]): Operation[S, List[Out]] =
          unit[S, OpDSL { type Self = S }]
            .flatMap(body)
            .flatMap {
              case result if n == 1 ⇒ Return((result :: acc).reverse)
              case result           ⇒ step(n - 1, result :: acc)
            }
        step(n, Nil)
      }
    }
    private object nextLoop extends NextLoop[Nothing]

    def apply[T]: Next[T] = next.asInstanceOf[Next[T]]

    trait Next[T] {
      def apply[U](body: OpDSL { type Self = T } ⇒ Operation[T, U]): Operation[T, U] =
        unit[T, OpDSL { type Self = T }].flatMap(body)
    }
    private object next extends Next[Nothing]

    trait NextStep[T] {
      def apply[U](mailboxCapacity: Int, body: OpDSL { type Self = T } ⇒ Operation[T, U])(implicit opDSL: OpDSL): Operation[opDSL.Self, U] =
        Call(Process("nextStep", Duration.Inf, mailboxCapacity, body(null)))
    }
    private[typed] object nextStep extends NextStep[Nothing]
  }

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

  /**
   * Helper to make `Operation.map` behave like `flatMap` when needed.
   */
  trait MapAdapter[Self, Out, Mapped] {
    def lift[O](f: O ⇒ Out): O ⇒ Operation[Self, Mapped]
  }
  /**
   * Helper to make `Operation.map` behave like `flatMap` when needed.
   */
  object MapAdapter extends MapAdapterLow {
    implicit def mapAdapterOperation[Self, M]: MapAdapter[Self, Operation[Self, M], M] =
      new MapAdapter[Self, Operation[Self, M], M] {
        override def lift[O](f: O ⇒ Operation[Self, M]): O ⇒ Operation[Self, M] = f
      }
  }
  /**
   * Helper to make `Operation.map` behave like `flatMap` when needed.
   */
  trait MapAdapterLow {
    implicit def mapAdapterAny[Self, Out]: MapAdapter[Self, Out, Out] =
      new MapAdapter[Self, Out, Out] {
        override def lift[O](f: O ⇒ Out): O ⇒ Operation[Self, Out] = o ⇒ Return(f(o))
      }
  }

  /**
   * A Process runs the given operation steps in a context that provides the
   * needed [[ActorRef]] of type `S` as the self-reference. Every process is
   * allotted a maximum lifetime after which it is canceled; you may set this
   * to `Duration.Inf` for a server process.
   */
  case class Process[S, +Out](name: String, timeout: Duration, mailboxCapacity: Int, operation: Operation[S, Out]) {
    a.ActorPath.validatePathElement(name)

    /**
     * Execute the given computation and process step after having completed
     * the current step. The current step’s computed value will be used as
     * input for the next computation.
     */
    def flatMap[T](f: Out ⇒ Operation[S, T]): Process[S, T] = copy(operation = FlatMap(operation, f))

    /**
     * Map the value computed by this process step by the given function,
     * flattening the result if it is an [[Operation]] (by executing the
     * operation and using its result as the mapped value).
     *
     * The reason behind flattening when possible is to allow the formulation
     * of infinite process loops (as performed for example by server processes
     * that respond to any number of requests) using for-comprehensions.
     * Without this flattening a final pointless `map` step would be added
     * for each iteration, eventually leading to an OutOfMemoryError.
     */
    def map[T, Mapped](f: Out ⇒ T)(implicit ev: MapAdapter[S, T, Mapped]): Process[S, Mapped] = flatMap(ev.lift(f))

    /**
     * Perform the given side-effect after this process step, continuing with
     * the `Unit` value.
     */
    def foreach(f: Out ⇒ Unit): Process[S, Unit] = flatMap(o ⇒ Return(f(o)))

    /**
     * Only continue this process if the given predicate is fulfilled, terminate
     * it otherwise.
     */
    def filter(p: Out ⇒ Boolean): Process[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)

    /**
     * Only continue this process if the given predicate is fulfilled, terminate
     * it otherwise.
     */
    def withFilter(p: Out ⇒ Boolean): Process[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)

    /**
     * Create a copy with modified timeout parameter.
     */
    def withTimeout(timeout: Duration): Process[S, Out] = copy(timeout = timeout)

    /**
     * Create a copy with modified mailbox capacity.
     */
    def withMailboxCapacity(mailboxCapacity: Int): Process[S, Out] = copy(mailboxCapacity = mailboxCapacity)

    /**
     * Convert to a runnable [[Behavior]].
     */
    def toBehavior: Behavior[ActorCmd[S]] = new internal.ProcessInterpreter(this)
  }

  /**
   * An Operation is a step executed by a [[Process]]. It exists in a context
   * characterized by the process’ ActorRef of type `S` and computes
   * a value of type `Out` when executed.
   */
  sealed trait Operation[S, +Out] {
    /**
     * Execute the given computation and process step after having completed
     * the current step. The current step’s computed value will be used as
     * input for the next computation.
     */
    def flatMap[T](f: Out ⇒ Operation[S, T]): Operation[S, T] = FlatMap(this, f)

    /**
     * Map the value computed by this process step by the given function,
     * flattening the result if it is an [[Operation]] (by executing the
     * operation and using its result as the mapped value).
     *
     * The reason behind flattening when possible is to allow the formulation
     * of infinite process loops (as performed for example by server processes
     * that respond to any number of requests) using for-comprehensions.
     * Without this flattening a final pointless `map` step would be added
     * for each iteration, eventually leading to an OutOfMemoryError.
     */
    def map[T, Mapped](f: Out ⇒ T)(implicit ev: MapAdapter[S, T, Mapped]): Operation[S, Mapped] = flatMap(ev.lift(f))

    /**
     * Unfortunately for-comprehensions desugar to `.foreach` if the `yield`
     * keyword is absent, but they do so throughout the whole expression.
     * Coding a normal signature for `foreach` would break everything after
     * the first step as it would not be executed—the `Unit` result is there
     * already.
     */
    def foreach[T, Mapped](f: Out ⇒ T)(implicit ev: MapAdapter[S, T, Mapped]): Operation[S, Mapped] = flatMap(ev.lift(f))

    /**
     * Only continue this process if the given predicate is fulfilled, terminate
     * it otherwise.
     */
    def filter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)

    /**
     * Only continue this process if the given predicate is fulfilled, terminate
     * it otherwise.
     */
    def withFilter(p: Out ⇒ Boolean): Operation[S, Out] = flatMap(o ⇒ if (p(o)) Return(o) else ShortCircuit)

    /**
     * Wrap as a [[Process]] with infinite timeout and a mailbox capacity of 1.
     * Small processes that are called or chained often interact in a fully
     * sequential fashion, where these defaults make sense.
     */
    def named(name: String): Process[S, Out] = Process(name, Duration.Inf, 1, this)

  }

  /*
   * These are the private values that make up the core algebra.
   */

  private[typed] case class FlatMap[S, Out1, Out2](first: Operation[S, Out1], then: Out1 ⇒ Operation[S, Out2]) extends Operation[S, Out2] {
    override def toString: String = s"FlatMap($first)"
  }
  private[typed] case object ShortCircuit extends Operation[Nothing, Nothing] {
    override def flatMap[T](f: Nothing ⇒ Operation[Nothing, T]): Operation[Nothing, T] = this
  }

  private[typed] case object System extends Operation[Nothing, ActorSystem[Nothing]]
  private[typed] case object Read extends Operation[Nothing, Nothing]
  private[typed] case object ProcessSelf extends Operation[Nothing, ActorRef[Any]]
  private[typed] case object ActorSelf extends Operation[Nothing, ActorRef[ActorCmd[Nothing]]]
  private[typed] case class Return[T](value: T) extends Operation[Nothing, T]
  private[typed] case class Call[S, T](process: Process[S, T]) extends Operation[Nothing, T]
  private[typed] case class Fork[S](process: Process[S, Any]) extends Operation[Nothing, SubActor[S]]
  private[typed] case class Spawn[S](process: Process[S, Any]) extends Operation[Nothing, ActorRef[ActorCmd[S]]]
  private[typed] case class Schedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T]) extends Operation[Nothing, a.Cancellable]
  private[typed] case class Replay[T](key: StateKey[T]) extends Operation[Nothing, T]
  private[typed] case class Snapshot[T](key: StateKey[T]) extends Operation[Nothing, T]
  private[typed] case class State[S, T <: StateKey[S], E](key: T, afterUpdates: Boolean, transform: S ⇒ (Seq[T#Event], E)) extends Operation[Nothing, E]
  private[typed] case class StateR[S, T <: StateKey[S]](key: T, afterUpdates: Boolean, transform: S ⇒ Seq[T#Event]) extends Operation[Nothing, S]
  private[typed] case class Forget[T](key: StateKey[T]) extends Operation[Nothing, akka.Done]

  // FIXME figure out cleanup of external resources after a failure

  /*
   * The core operations: keep these minimal!
   */

  /**
   * Obtain a reference to the ActorSystem in which this process is running.
   */
  def opSystem(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorSystem[Nothing]] =
    System

  /**
   * Read a message from this process’ input channel.
   */
  def opRead(implicit opDSL: OpDSL): Operation[opDSL.Self, opDSL.Self] =
    Read

  /**
   * Obtain this process’ [[ActorRef]], not to be confused with the ActorRef of the Actor this process is running in.
   */
  def opProcessSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[opDSL.Self]] =
    ProcessSelf

  /**
   * Obtain the [[ActorRef]] of the Actor this process is running in.
   */
  def opActorSelf(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Nothing]]] =
    ActorSelf

  /**
   * Lift a plain value into a process that returns that value.
   */
  def opUnit[U](value: U)(implicit opDSL: OpDSL): Operation[opDSL.Self, U] =
    Return(value)

  /**
   * Execute the given process within the current Actor, await and return that process’ result.
   */
  def opCall[Self, Out](process: Process[Self, Out])(implicit opDSL: OpDSL): Operation[opDSL.Self, Out] =
    Call(process)

  /**
   * Create and execute a process with a self reference of the given type,
   * await and return that process’ result. This is equivalent to creating
   * a process with [[OpDSL]] and using `call` to execute it.
   */
  def opNextStep[T] =
    OpDSL.nextStep.asInstanceOf[OpDSL.NextStep[T]]

  /**
   * Execute the given process within the current Actor, concurrently with the
   * current process. The value computed by the forked process cannot be
   * observed, instead you would have the forked process send a message to the
   * current process to communicate results. The returned [[SubActor]] reference
   * can be used to send messages to the forked process or to cancel it.
   */
  def opFork[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[Self]] =
    Fork(process)

  /**
   * Execute the given process in a newly spawned child Actor of the current
   * Actor. The new Actor is fully encapsulated behind the [[ActorRef]] that
   * is returned.
   */
  def opSpawn[Self](process: Process[Self, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, ActorRef[ActorCmd[Self]]] =
    Spawn(process)

  /**
   * Schedule a message to be sent after the given delay has elapsed.
   */
  def opSchedule[T](delay: FiniteDuration, msg: T, target: ActorRef[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, a.Cancellable] =
    Schedule(delay, msg, target)

  private val _any2Nil = (state: Any) ⇒ Nil → state
  private def any2Nil[T] = _any2Nil.asInstanceOf[T ⇒ (Nil.type, T)]

  /**
   * Read the state stored for the given [[StateKey]], suspending this process
   * until after all outstanding updates for the key have been completed if
   * `afterUpdates` is `true`.
   */
  def opReadState[T](key: StateKey[T], afterUpdates: Boolean = true)(implicit opDSL: OpDSL): Operation[opDSL.Self, T] =
    State[T, StateKey[T], T](key, afterUpdates, any2Nil)

  /**
   * Update the state stored for the given [[StateKey]] by emitting events that
   * are applied to the state in order, suspending this process
   * until after all outstanding updates for the key have been completed if
   * `afterUpdates` is `true`.
   */
  def opUpdateState[T, E](key: StateKey[T], afterUpdates: Boolean = true)(
    transform: T ⇒ (Seq[key.Event], E))(implicit opDSL: OpDSL): Operation[opDSL.Self, E] =
    State(key, afterUpdates, transform)

  /**
   * Update the state by emitting a sequence of events, returning the updated state. The
   * process is suspended until after all outstanding updates for the key have been
   * completed if `afterUpdates` is `true`.
   */
  def opUpdateAndReadState[T](key: StateKey[T], afterUpdates: Boolean = true)(
    transform: T ⇒ Seq[key.Event])(implicit opDSL: OpDSL): Operation[opDSL.Self, T] =
    StateR(key, afterUpdates, transform)

  /**
   * Instruct the Actor to persist the state for the given [[StateKey]] after
   * all currently outstanding updates for this key have been completed,
   * suspending this process until done.
   */
  def opTakeSnapshot[T](key: PersistentStateKey[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, T] =
    Snapshot(key)

  /**
   * Restore the state for the given [[StateKey]] from persistent event storage.
   * If a snapshot is found it will be used as the starting point for the replay,
   * otherwise events are replayed from the beginning of the event log, starting
   * with the given initial data as the state before the first event is applied.
   */
  def opReplayPersistentState[T](key: PersistentStateKey[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, T] =
    Replay(key)

  /**
   * Remove the given [[StateKey]] from this Actor’s storage. The slot can be
   * filled again using `updateState` or `replayPersistentState`.
   */
  def opForgetState[T](key: StateKey[T])(implicit opDSL: OpDSL): Operation[opDSL.Self, akka.Done] =
    Forget(key)

  /*
   * State Management
   */

  /**
   * A key into the Actor’s state map that allows access both for read and
   * update operations. Updates are modeled by emitting events of the specified
   * type. The updates are applied to the state in the order in which they are
   * emitted. For persistent state data please refer to `PersistentStateKey`
   * and for ephemeral non-event-sourced data take a look at `SimpleStateKey`.
   */
  sealed trait StateKey[T] {
    type Event
    def apply(state: T, event: Event): T
    def initial: T
  }

  /**
   * Event type emitted in conjunction with [[SimpleStateKey]], the only
   * implementation is [[SetState]].
   */
  sealed trait SetStateEvent[T] {
    def value: T
  }
  /**
   * Event type that instructs the state of a [[SimpleStateKey]] to be
   * replaced with the given value.
   */
  final case class SetState[T](override val value: T) extends SetStateEvent[T] with Seq[SetStateEvent[T]] {
    def iterator: Iterator[akka.typed.ScalaProcess.SetStateEvent[T]] = Iterator.single(this)
    def apply(idx: Int): akka.typed.ScalaProcess.SetStateEvent[T] =
      if (idx == 0) this
      else throw new IndexOutOfBoundsException
    def length: Int = 1
  }

  /**
   * Use this key for state that shall neither be persistent nor event-sourced.
   * In effect this turns `updateState` into access to a State monad identified
   * by this key instance.
   *
   * Beware that reference equality is used to identify this key: you should
   * create the key as a `val` inside a top-level `object`.
   */
  final class SimpleStateKey[T](override val initial: T) extends StateKey[T] {
    type Event = SetStateEvent[T]
    def apply(state: T, event: SetStateEvent[T]) = event.value
    override def toString: String = f"SimpleStateKey@$hashCode%08X($initial)"
  }

  /**
   * The data for a [[StateKey]] of this kind can be made persistent by
   * invoking `replayPersistentState`. Persistence is achieved by writing all
   * emitted events to the Akka Persistence Journal.
   */
  trait PersistentStateKey[T] extends StateKey[T] {
    def clazz: Class[Event]
  }

  /*
   * Derived operations
   */
  def firstOf[T](timeout: Duration, processes: Process[_, T]*)(implicit opDSL: OpDSL): Operation[opDSL.Self, T] = {
    def forkAll(self: ActorRef[T], index: Int = 0,
                p:   List[Process[_, T]]     = processes.toList,
                acc: List[SubActor[Nothing]] = Nil)(implicit opDSL: OpDSL { type Self = T }): Operation[T, List[SubActor[Nothing]]] =
      p match {
        case Nil ⇒ opUnit(acc)
        case x :: xs ⇒
          opFork(x.copy(name = s"$index-${x.name}").map(self ! _))
            .map(sub ⇒ forkAll(self, index + 1, xs, sub :: acc))
      }
    opCall(Process("firstOf", timeout, processes.size, OpDSL[T] { implicit opDSL ⇒
      for {
        self ← opProcessSelf
        subs ← forkAll(self)
        value ← opRead
      } yield {
        subs.foreach(_.cancel())
        value
      }
    }))
  }

  def delay[T](time: FiniteDuration, value: T): Operation[T, T] =
    OpDSL[T] { implicit opDSL ⇒
      for {
        self ← opProcessSelf
        _ ← opSchedule(time, value, self)
      } yield opRead
    }

  def forkAndCancel[T](timeout: FiniteDuration, process: Process[T, Any])(implicit opDSL: OpDSL): Operation[opDSL.Self, SubActor[T]] =
    for {
      sub ← opFork(process)
      _ ← opFork(Process("cancelAfter", Duration.Inf, 1, delay(timeout, ()).foreach(_ ⇒ sub.cancel())))
    } yield opUnit(sub)

  def retry[S, T](timeout: FiniteDuration, retries: Int, ops: Process[S, T])(implicit opDSL: OpDSL): Operation[opDSL.Self, T] = {
    firstOf(Duration.Inf, ops.map(Some(_)), delay(timeout, None).named("retryTimeout"))
      .map {
        case Some(res)           ⇒ opUnit(res)
        case None if retries > 0 ⇒ retry(timeout, retries - 1, ops)
        case None                ⇒ throw new RetriesExceeded
      }
  }

  sealed trait ActorCmd[+T]
  case class MainCmd[+T](cmd: T) extends ActorCmd[T]
  private[typed] trait InternalActorCmd[+T] extends ActorCmd[T]

  trait SubActor[-T] {
    def ref: ActorRef[T]
    def cancel(): Unit
  }
}
