/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import scala.concurrent.Await
import scala.concurrent.Future

import akka.Done
import akka.testkit.AkkaSpec
import com.typesafe.config.ConfigFactory
import akka.actor.CoordinatedShutdown.Phase

object CoordinatedShutdownSpec {

}

class CoordinatedShutdownSpec extends AkkaSpec {

  def extSys = system.asInstanceOf[ExtendedActorSystem]

  "CoordinatedShutdown" must {

    "sort phases in topolgical order" in {
      CoordinatedShutdown.topologicalSort(Map.empty) should ===(Nil)

      CoordinatedShutdown.topologicalSort(Map(
        "a" → Phase(Set.empty))) should ===(List("a"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Phase(Set("a")))) should ===(List("a", "b"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → Phase(Set("a")), "b" → Phase(Set("a")))).head should ===("a") // b, c can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Phase(Set("a")), "c" → Phase(Set("b")))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Phase(Set("a")), "c" → Phase(Set("a", "b")))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → Phase(Set("a", "b")))).last should ===("c") // a, b can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Phase(Set("a")), "c" → Phase(Set("b")), "d" → Phase(Set("b", "c")),
        "e" → Phase(Set("d")))) should ===(
        List("a", "b", "c", "d", "e"))

      val result = CoordinatedShutdown.topologicalSort(Map(
        "a2" → Phase(Set("a1")), "a3" → Phase(Set("a2")),
        "b2" → Phase(Set("b1")), "b3" → Phase(Set("b2"))))
      val (a, b) = result.partition(_.charAt(0) == 'a')
      a should ===(List("a1", "a2", "a3"))
      b should ===(List("b1", "b2", "b3"))
    }

    "detect cycles in phases (non-DAG)" in {
      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "b" → Phase(Set("a")), "a" → Phase(Set("b"))))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "c" → Phase(Set("a")), "c" → Phase(Set("b")), "b" → Phase(Set("c"))))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "d" → Phase(Set("a")), "d" → Phase(Set("c")), "c" → Phase(Set("b")), "b" → Phase(Set("d"))))
      }

    }

    "run ordered phases" in {
      import system.dispatcher
      val phases = Map(
        "a" → Phase(Set.empty),
        "b" → Phase(Set("a")),
        "c" → Phase(Set("b", "a")))
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
      val phases = Map("a" → Phase(Set.empty))
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

    "parse phases from config" in {
      CoordinatedShutdown.phasesFromConfig(ConfigFactory.parseString("""
        a = {}
        b {
         depends-on = [a]
        }
        c {
         depends-on = [a, b]
        }
        """)) should ===(Map(
        "a" → Phase(Set.empty),
        "b" → Phase(Set("a")),
        "c" → Phase(Set("b", "a"))))
    }

  }

}
