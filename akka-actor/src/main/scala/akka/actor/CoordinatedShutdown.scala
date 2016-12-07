/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
import com.typesafe.config.Config

object CoordinatedShutdown extends ExtensionId[CoordinatedShutdown] with ExtensionIdProvider {
  override def get(system: ActorSystem): CoordinatedShutdown = super.get(system)

  override def lookup = CoordinatedShutdown

  override def createExtension(system: ExtendedActorSystem): CoordinatedShutdown = {
    val phases = phasesFromConfig(system.settings.config.getConfig("akka.coordinated-shutdown-phases"))
    new CoordinatedShutdown(system, phases)
  }

  /**
   * INTERNAL API
   */
  private[akka] final case class Phase(dependsOn: Set[String])

  /**
   * INTERNAL API
   */
  private[akka] def phasesFromConfig(conf: Config): Map[String, Phase] = {
    import scala.collection.JavaConverters._
    conf.root.unwrapped.asScala.toMap.map {
      case (k, v: java.util.Map[_, _]) ⇒
        val dependsOn = v.get("depends-on") match {
          case null                 ⇒ Set.empty[String]
          case d: java.util.List[_] ⇒ d.asScala.map(_.toString).toSet
          case d ⇒
            throw new IllegalArgumentException(s"Expected list value for [$k.depends-on], got [$v]")
        }
        k → Phase(dependsOn)
      case (k, v) ⇒
        throw new IllegalArgumentException(s"Expected object value for [$k], got [$v]")
    }
  }

  /**
   * INTERNAL API: https://en.wikipedia.org/wiki/Topological_sorting
   */
  private[akka] def topologicalSort(phases: Map[String, Phase]): List[String] = {
    var result = List.empty[String]
    var unmarked = phases.keySet ++ phases.values.flatMap(_.dependsOn) // in case phase is not defined as key
    var tempMark = Set.empty[String] // for detecting cycles

    while (unmarked.nonEmpty) {
      depthFirstSearch(unmarked.head)
    }

    def depthFirstSearch(u: String): Unit = {
      if (tempMark(u))
        throw new IllegalArgumentException("Cycle detected in graph of phases. It must be a DAG. " + phases)
      if (unmarked(u)) {
        tempMark += u
        phases.get(u) match {
          case Some(Phase(dependsOn)) ⇒ dependsOn.foreach(depthFirstSearch)
          case None                   ⇒
        }
        unmarked -= u // permanent mark
        tempMark -= u
        result = u :: result
      }
    }

    result.reverse
  }

}

class CoordinatedShutdown private[akka] (system: ExtendedActorSystem, phases: Map[String, CoordinatedShutdown.Phase])
  extends Extension {

  private val knownPhases = phases.keySet ++ phases.values.flatMap(_.dependsOn)
  private val orderedPhases = CoordinatedShutdown.topologicalSort(phases)
  private val tasks = new ConcurrentHashMap[String, Vector[() ⇒ Future[Done]]]
  private val runStarted = new AtomicBoolean(false)
  private val runPromise = Promise[Done]()

  def addTask(phase: String)(task: () ⇒ Future[Done]): Unit = {
    require(
      knownPhases(phase),
      s"unknown phase [$phase], known phases [$knownPhases]. All phases must be defined in configuration")
    val current = tasks.get(phase)
    if (current == null) {
      if (tasks.putIfAbsent(phase, Vector(task)) != null)
        addTask(phase)(task) // CAS failed, retry
    } else {
      if (!tasks.replace(phase, current, current :+ task))
        addTask(phase)(task) // CAS failed, retry
    }
  }

  def addNotification(phase: String, ref: ActorRef, message: Any): Unit = {
    addTask(phase) {
      () ⇒
        ref ! message
        Future.successful(Done)
    }
  }

  def run(): Future[Done] = {
    if (runStarted.compareAndSet(false, true)) {
      import system.dispatcher
      def loop(remainingPhases: List[String]): Future[Done] = {
        remainingPhases match {
          case Nil ⇒ Future.successful(Done)
          case phase :: remaining ⇒
            (tasks.get(phase) match {
              case null ⇒ Future.successful(Done)
              case tasks ⇒
                // not that tasks within same phase are performed in parallel
                Future.sequence(tasks.map(_.apply())).map(_ ⇒ Done)
            }).flatMap(_ ⇒ loop(remaining))
        }
      }
      val done = loop(orderedPhases)
      runPromise.completeWith(done)
    }
    runPromise.future
  }

}
