/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed
package internal

import akka.{ actor ⇒ a }
import java.util.concurrent.ArrayBlockingQueue
import ScalaProcess._
import ScalaDSL._
import scala.concurrent.duration.FiniteDuration
import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.runtime.BoxedUnit
import scala.runtime.BoxesRunTime
import java.util.LinkedList

/**
 * Implementation notes:
 *
 *  - a process is a tree of AST nodes, where each leaf is a producer of a process
 *    (in fact the inner nodes are all FlatMap)
 *  - interpreter has a list of currently existing processes
 *  - processes may be active (i.e. waiting for external input) or passive (i.e.
 *    waiting for internal progress)
 *  - external messages and internal completions are events that are enqueued
 *  - event processing runs in FIFO order, which ensures some fairness between
 *    concurrent processes
 *  - what is stored is actually a Traversal of a tree which keeps the back-links
 *    pointing towards the root; this is cheaper than rewriting the trees
 *  - the maximum event queue size is bounded by #channels + #processes (multiple
 *    events from the same channel can be coalesced inside that channel by a counter)
 *  - this way even complex process trees can be executed with minimal allocations
 *    (fixed-size preallocated arrays for event queue and back-links, preallocated
 *    processes can even be reentrant due to separate unique Traversals)
 */
private[typed] object ProcessInterpreter {

  sealed trait TraversalState
  case object HasValue extends TraversalState
  case object NeedsTrampoline extends TraversalState
  case object NeedsExternalInput extends TraversalState
  case object NeedsInternalInput extends TraversalState

  case object Canceled

  sealed trait Input // could be a channel or a timer
  final class Timer(delay: FiniteDuration, actorContext: ActorContext[ActorCmd[Nothing]])
      extends Input with InternalActorCmd[Nothing] {
    val cancelable = actorContext.schedule(delay, actorContext.self, this)
  }

  val Debug = true
}

private[typed] class ProcessInterpreter[T](initial: => Process[T, Any]) extends Behavior[ActorCmd[T]] {
  import ProcessInterpreter._

  // FIXME data structures to be optimized
  private var _internalTriggers = Map.empty[Traversal[_], Traversal[_]]
  private var _externalTriggers = Map.empty[Timer, Traversal[_]]
  private val queue = new LinkedList[Traversal[_]]
  private var processCount = 0

  def management(ctx: ActorContext[ActorCmd[T]], msg: Signal): Behavior[ActorCmd[T]] = {
    msg match {
      case PreStart =>
        new Traversal(initial, ctx)
        execute(ctx)
      case PostStop ⇒
        // FIXME clean everything up
        Same
      case Terminated(ref) ⇒
        // FIXME add ability to watch things
        Same
      case _ ⇒ Same
    }
  }

  def message(ctx: ActorContext[ActorCmd[T]], msg: ActorCmd[T]): Behavior[ActorCmd[T]] = {
    msg match {
      case t: Traversal[_] ⇒
        if (t.isAlive) {
          t.registerReceipt()
          if (t.state == NeedsExternalInput) {
            t.dispatchInput(t.receiveOne(), t)
            triggerCompletions(ctx, t)
          }
        }
        execute(ctx)
      case timer: Timer ⇒
        _externalTriggers.get(timer) match {
          case None ⇒
          case Some(t) ⇒
            _externalTriggers -= timer
            t.dispatchInput((), timer)
            triggerCompletions(ctx, t)
        }
        execute(ctx)
    }
  }

  /**
   * Consume the queue of outstanding triggers.
   */
  private def execute(ctx: ActorContext[ActorCmd[T]]): Behavior[ActorCmd[T]] = {
    while (!queue.isEmpty()) {
      val traversal = queue.poll()
      if (Debug) println(s"${ctx.self} running $traversal")
      if (traversal.state == NeedsTrampoline) traversal.dispatchTrampoline()
      triggerCompletions(ctx, traversal)
    }
    if (_internalTriggers.isEmpty && _externalTriggers.isEmpty) Stopped else Same
  }

  /**
   * This only notifies potential listeners of the computed value of a finished
   * process; the process must clean itself up beforehand.
   */
  @tailrec private def triggerCompletions(ctx: ActorContext[ActorCmd[T]], traversal: Traversal[_]): Unit =
    if (traversal.state == HasValue) {
      if (Debug) println(s"${ctx.self} finished $traversal")
      _internalTriggers.get(traversal) match {
        case None ⇒ // nobody listening
        case Some(t) ⇒
          _internalTriggers -= traversal
          t.dispatchInput(traversal.getValue, traversal)
          triggerCompletions(ctx, t)
      }
    }

  private class Traversal[Tself](process: Process[Tself, Any], ctx: ActorContext[ActorCmd[T]])
      extends InternalActorCmd[Nothing] with ProcessInterpreter.Input with Function1[Tself, ActorCmd[T]]
      with SubActor[Tself] {

    /*
     * Implementation of the queue aspect and InternalActorCmd as well as for spawnAdapter
     */

    private val mailQueue = new ArrayBlockingQueue[Tself](process.mailboxCapacity) // FIXME replace with lock-free queue
    private var toRead = 0

    val parent = ctx.self

    def registerReceipt(): Unit = toRead += 1
    def canReceive: Boolean = toRead > 0
    def receiveOne(): Tself = mailQueue.poll()
    def isAlive: Boolean = toRead >= 0

    def apply(msg: Tself): ActorCmd[T] =
      if (mailQueue.offer(msg)) this
      else null // adapter drops nulls

    override val ref: ActorRef[Tself] = ctx.watch(ctx.spawnAdapter(this))

    /*
     * Implementation of traversal logic
     */

    if (Debug) println(s"${ctx.self} new traversal for $process")

    override def toString: String = if (Debug) s"Traversal($process, $state, ${stack.toList}, $ptr)" else super.toString

    @tailrec private def depth(op: Operation[_, Any] = process.operation, d: Int = 0): Int =
      op match {
        case FlatMap(next, _) => depth(next, d + 1)
        case Read | Call(_)   => d + 2
        case _                => d + 1
      }

    /*
     * The state defines what is on the stack:
     *  - HasValue means stack only contains the single end result
     *  - NeedsTrampoline: pop value, then pop operation that needs it
     *  - NeedsExternalInput: pop valueOrInput, then pop operation
     *  - NeedsInternalInput: pop valueOrTraversal, then pop operation
     */
    private var stack = new Array[AnyRef](Math.max(depth(), 5))
    private var ptr = 0
    private var _state: TraversalState = initialize(process.operation)

    def awaitsMessage = _state == NeedsExternalInput && peek() == this

    private def push(v: Any): Unit = {
      stack(ptr) = v.asInstanceOf[AnyRef]
      ptr += 1
    }
    private def pop(): AnyRef = {
      ptr -= 1
      val ret = stack(ptr)
      stack(ptr) = null
      ret
    }
    private def peek(): AnyRef =
      if (ptr == 0) null else stack(ptr - 1)
    private def ensureSpace(n: Int): Unit =
      if (stack.length - ptr < n) {
        val larger = new Array[AnyRef](n + ptr)
        java.lang.System.arraycopy(stack, 0, larger, 0, ptr)
        stack = larger
      }

    private def valueOrTrampoline() =
      if (ptr == 1) HasValue
      else {
        queue.add(this)
        NeedsTrampoline
      }

    private def triggerOn(i: Input): i.type = {
      i match {
        case t: Timer        => _externalTriggers += (t -> this)
        case t: Traversal[_] => _internalTriggers += (t -> this)
      }
      i
    }

    def getValue: Any = {
      assert(_state == HasValue)
      stack(0)
    }

    /**
     * Obtain the current state for this Traversal.
     */
    def state: TraversalState = _state

    @tailrec private def initialize(node: Operation[_, Any]): TraversalState =
      node match {
        case FlatMap(first, _) ⇒
          push(node)
          initialize(first)
        case System =>
          push(ctx.system)
          valueOrTrampoline()
        case Read ⇒
          push(node)
          push(this)
          NeedsExternalInput
        case Self =>
          push(ref)
          valueOrTrampoline()
        case ActorSelf =>
          push(ctx.self)
          valueOrTrampoline()
        case Return(value) =>
          push(value)
          valueOrTrampoline()
        case Call(process) =>
          push(node)
          push(triggerOn(new Traversal(process, ctx)))
          NeedsInternalInput
        case Fork(other) ⇒
          push(new Traversal(other, ctx))
          valueOrTrampoline()
        case Spawn(Process(name, timeout, mailboxCapacity, ops)) =>
          // FIXME make dispatcher configurable
          push(ctx.spawn(toBehavior(ops), name, MailboxCapacity(mailboxCapacity)))
          valueOrTrampoline()
        case Schedule(delay, msg, target) ⇒
          push(ctx.schedule(delay, target, msg))
          valueOrTrampoline()
      }

    def dispatchInput(value: Any, source: Traversal[_]): Unit = {
      if (Debug) println(s"${ctx.self} dispatching input $value from $source to $this")
      assert(_state == NeedsInternalInput)
      assert(source eq pop())
      push(value)
      _state = valueOrTrampoline()
    }

    def dispatchInput(value: Any, source: Input): Unit = {
      if (Debug) println(s"${ctx.self} dispatching input $value from $source to $this")
      assert(_state == NeedsExternalInput)
      assert(source eq pop())
      pop() match {
        case Read ⇒
          push(value)
          _state = valueOrTrampoline()
      }
    }

    def dispatchTrampoline(): Unit = {
      assert(_state == NeedsTrampoline)
      pop() match {
        // might need to reinsert the Termination.Sentinel logic later again
        case value ⇒
          pop() match {
            case FlatMap(_, cont) ⇒
              val contOps = cont(value)
              ensureSpace(depth(contOps))
              _state = initialize(contOps)
          }
      }
    }

    def cancel(): Unit = {
      if (Debug) println(s"${ctx.self} canceling $this")
      if (isAlive) ref.sorry.sendSystem(Terminate())
      toRead = -1
      _state match {
        case HasValue        ⇒ // nothing to do
        case NeedsTrampoline ⇒ stack(ptr - 1) = Canceled
        case NeedsExternalInput ⇒
          pop() match {
            case t: Traversal[_] ⇒
            case t: Timer ⇒
              t.cancelable.cancel()
              _externalTriggers -= t
          }
          pop()
          push(Canceled)
          _state = valueOrTrampoline()
        case NeedsInternalInput ⇒
          pop().asInstanceOf[Traversal[_]].cancel()
          push(Canceled)
          _state = valueOrTrampoline()
      }
    }
  }

}
