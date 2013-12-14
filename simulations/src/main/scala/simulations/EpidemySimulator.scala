package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8
    val timeToBecomeSick: Int = 6
    val timeToDie: Int = 14
    val timeToBecomeImmune: Int = 16
    val timeToBecomeHealthy: Int = 18
    val dyingProbability: Int = 25  // in percent
    val maxTimeForMove: Int = 5
    val transmissibilityRate: Int = 40  // in percent
    val prevalenceRate: Int = 1 // in percent
  }

  import SimConfig._

  def initPersonsList: List[Person] = {
    val res = (for {
      i <- 1 to population
    } yield {
      val person = new Person(i)
      person.row = randomBelow(roomRows)
      person.col = randomBelow(roomColumns)
      afterDelay(randomBelow(5) + 1) {
        move(person)
      }
      person
    }).toList
    val personsToInfect = population / (prevalenceRate * 100)
    for {
      i <- 1 to personsToInfect
    } yield {
      val notInfected = res.filter(!_.infected)
      notInfected(randomBelow(notInfected.size)).infect
    }
    res
  }

  val persons: List[Person] = initPersonsList

  class Person (val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    def isInfectious = infected | sick | immune | dead
    def isVisiblyInfectious = sick | dead

    def infect {
      infected = true
      afterDelay(timeToBecomeSick) {
        sick = true
      }
      if (randomBelow(100) < dyingProbability) {
        afterDelay(timeToDie) {
          dead = true
        }
      }
      afterDelay(timeToBecomeImmune) {
        if (!dead) {
          sick = false
          immune = true
        }
      }
      afterDelay(timeToBecomeHealthy) {
        if (!dead) {
          infected = false
          immune = false
        }
      }
    }
  }

  type Move = (Int, Int)

  def move(p: Person) {
    if (!p.dead) {
      def areVisiblyInfectedPeopleInRoom(m: Move): Boolean =
        personsInRoom((p.row + m._1 + roomRows) % roomRows, (p.col + m._2 + roomColumns) % roomColumns).count(_.isVisiblyInfectious) > 0
      def areInfectedPeopleInRoom(p: Person): Boolean = personsInRoom(p.row, p.col).count(_.isInfectious) > 0

      val possibleMoves = List(new Move(0, 1), new Move(0, -1), new Move(1, 0), new Move(-1, 0)).filterNot(areVisiblyInfectedPeopleInRoom)

      if (!possibleMoves.isEmpty) {
        val m = possibleMoves(randomBelow(possibleMoves.size))
        p.row = (p.row + m._1 + roomRows) % roomRows
        p.col = (p.col + m._2 + roomColumns) % roomColumns
      }
      if (!p.infected && !p.sick && !p.immune && areInfectedPeopleInRoom(p)) {
        if (randomBelow(100) < transmissibilityRate) {
          p.infect
        }
      }
      afterDelay(randomBelow(5) + 1) {
        move(p)
      }
    }
  }

  private def personsInRoom(row: Int, col: Int): List[Person] = persons.filter(_.row == row).filter(_.col == col)
}
