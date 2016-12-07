/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future

import akka.Done
import akka.testkit.AkkaSpec
import com.typesafe.config.ConfigFactory
import akka.actor.CoordinatedShutdown.Phase
import scala.concurrent.Promise
import java.util.concurrent.TimeoutException

class CoordinatedShutdownSpec extends AkkaSpec {

  def extSys = system.asInstanceOf[ExtendedActorSystem]

  // some convenience to make the test readable
  def phase(dependsOn: String*): Phase = Phase(dependsOn.toSet, timeout = 10.seconds, recover = true)
  val emptyPhase: Phase = Phase(Set.empty, timeout = 10.seconds, recover = true)

  "CoordinatedShutdown" must {

    "sort phases in topolgical order" in {
      CoordinatedShutdown.topologicalSort(Map.empty) should ===(Nil)

      CoordinatedShutdown.topologicalSort(Map(
        "a" → emptyPhase)) should ===(List("a"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → phase("a"))) should ===(List("a", "b"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → phase("a"), "b" → phase("a"))).head should ===("a") // b, c can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → phase("a"), "c" → phase("b"))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → phase("a"), "c" → phase("a", "b"))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → phase("a", "b"))).last should ===("c") // a, b can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → phase("a"), "c" → phase("b"), "d" → phase("b", "c"),
        "e" → phase("d"))) should ===(
        List("a", "b", "c", "d", "e"))

      val result = CoordinatedShutdown.topologicalSort(Map(
        "a2" → phase("a1"), "a3" → phase("a2"),
        "b2" → phase("b1"), "b3" → phase("b2")))
      val (a, b) = result.partition(_.charAt(0) == 'a')
      a should ===(List("a1", "a2", "a3"))
      b should ===(List("b1", "b2", "b3"))
    }

    "detect cycles in phases (non-DAG)" in {
      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "b" → phase("a"), "a" → phase("b")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "c" → phase("a"), "c" → phase("b"), "b" → phase("c")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "d" → phase("a"), "d" → phase("c"), "c" → phase("b"), "b" → phase("d")))
      }

    }

    "run ordered phases" in {
      import system.dispatcher
      val phases = Map(
        "a" → emptyPhase,
        "b" → phase("a"),
        "c" → phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a") { () ⇒
        testActor ! "A"
        Future.successful(Done)
      }
      co.addTask("b") { () ⇒
        testActor ! "B"
        Future.successful(Done)
      }
      co.addTask("b") { () ⇒
        Future {
          // to verify that c is not performed before b
          Thread.sleep(100)
          testActor ! "B"
          Done
        }
      }
      co.addTask("c") { () ⇒
        testActor ! "C"
        Future.successful(Done)
      }
      Await.result(co.run(), remainingOrDefault)
      receiveN(4) should ===(List("A", "B", "B", "C"))
    }

    "only run once" in {
      import system.dispatcher
      val phases = Map("a" → emptyPhase)
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a") { () ⇒
        testActor ! "A"
        Future.successful(Done)
      }
      Await.result(co.run(), remainingOrDefault)
      expectMsg("A")
      Await.result(co.run(), remainingOrDefault)
      testActor ! "done"
      expectMsg("done") // no additional A
    }

    "continue after timeout or failure" in {
      import system.dispatcher
      val phases = Map(
        "a" → emptyPhase,
        "b" → Phase(dependsOn = Set("a"), timeout = 100.millis, recover = true),
        "c" → phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("a") { () ⇒
        testActor ! "A"
        Future.failed(new RuntimeException("boom"))
      }
      co.addTask("b") { () ⇒
        testActor ! "B"
        Promise[Done]().future // never completed
      }
      co.addTask("c") { () ⇒
        testActor ! "C"
        Future.successful(Done)
      }
      Await.result(co.run(), remainingOrDefault)
      expectMsg("A")
      expectMsg("B")
      expectMsg("C")
    }

    "abort if recover=off" in {
      import system.dispatcher
      val phases = Map(
        "b" → Phase(dependsOn = Set("a"), timeout = 100.millis, recover = false),
        "c" → phase("b", "a"))
      val co = new CoordinatedShutdown(extSys, phases)
      co.addTask("b") { () ⇒
        testActor ! "B"
        Promise[Done]().future // never completed
      }
      co.addTask("c") { () ⇒
        testActor ! "C"
        Future.successful(Done)
      }
      val result = co.run()
      expectMsg("B")
      intercept[TimeoutException] {
        Await.result(result, remainingOrDefault)
      }
      expectNoMsg(200.millis) // C not run
    }

    "parse phases from config" in {
      CoordinatedShutdown.phasesFromConfig(ConfigFactory.parseString("""
        default-timeout = 10s
        phases {
          a = {}
          b {
            depends-on = [a]
            timeout = 15s
          }
          c {
            depends-on = [a, b]
            recover = off
          }
        }
        """)) should ===(Map(
        "a" → Phase(dependsOn = Set.empty, timeout = 10.seconds, recover = true),
        "b" → Phase(dependsOn = Set("a"), timeout = 15.seconds, recover = true),
        "c" → Phase(dependsOn = Set("a", "b"), timeout = 10.seconds, recover = false)))
    }

  }

}
