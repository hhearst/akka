/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor

import scala.concurrent.Await
import scala.concurrent.Future

import akka.Done
import akka.testkit.AkkaSpec
import com.typesafe.config.ConfigFactory

object CoordinatedShutdownSpec {

}

class CoordinatedShutdownSpec extends AkkaSpec {

  def extSys = system.asInstanceOf[ExtendedActorSystem]

  "CoordinatedShutdown" must {

    "sort phases in topolgical order" in {
      CoordinatedShutdown.topologicalSort(Map.empty) should ===(Nil)

      CoordinatedShutdown.topologicalSort(Map(
        "a" → Set.empty)) should ===(List("a"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Set("a"))) should ===(List("a", "b"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → Set("a"), "b" → Set("a"))).head should ===("a") // b, c can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Set("a"), "c" → Set("b"))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Set("a"), "c" → Set("a", "b"))) should ===(List("a", "b", "c"))

      CoordinatedShutdown.topologicalSort(Map(
        "c" → Set("a", "b"))).last should ===("c") // a, b can be in any order

      CoordinatedShutdown.topologicalSort(Map(
        "b" → Set("a"), "c" → Set("b"), "d" → Set("b", "c"), "e" → Set("d"))) should ===(
        List("a", "b", "c", "d", "e"))

      val result = CoordinatedShutdown.topologicalSort(Map(
        "a2" → Set("a1"), "a3" → Set("a2"),
        "b2" → Set("b1"), "b3" → Set("b2")))
      val (a, b) = result.partition(_.charAt(0) == 'a')
      a should ===(List("a1", "a2", "a3"))
      b should ===(List("b1", "b2", "b3"))
    }

    "detect cycles in phases (non-DAG)" in {
      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "b" → Set("a"), "a" → Set("b")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "c" → Set("a"), "c" → Set("b"), "b" → Set("c")))
      }

      intercept[IllegalArgumentException] {
        CoordinatedShutdown.topologicalSort(Map(
          "d" → Set("a"), "d" → Set("c"), "c" → Set("b"), "b" → Set("d")))
      }

    }

    "run ordered phases" in {
      import system.dispatcher
      val phases = Map(
        "a" → Set.empty[String],
        "b" → Set("a"),
        "c" → Set("b", "a"))
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
      val phases = Map("a" → Set.empty[String])
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
        a = []
        b = [a]
        c = [a, b]
      """)) should ===(Map(
        "a" → Set.empty[String],
        "b" → Set("a"),
        "c" → Set("b", "a")))
    }

  }

}
