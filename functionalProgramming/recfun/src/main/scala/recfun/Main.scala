package recfun
import common._

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = ???

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = ???

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = ???
}
ef balance(chars: List[Char]): Boolean = {
		def numericBalance(chars: List[Char], balanceSoFar: Int): Int = {
			if (chars.isEmpty) {
				0
			}
			else {
				val newBalance = chars.head match {
					case ')' => -1
					case '(' => 1
					case _ => 0
				}
				val rest = numericBalance(chars.tail, balanceSoFar + newBalance)
				if (balanceSoFar + newBalance < 0) {
					throw new IllegalStateException()
				} else {
					newBalance + rest
				}
			}
		}
		try {
			numericBalance(chars, 0) == 0
		} catch {
			case ise: IllegalStateException => {
				false
			}
		}
	}

	/**
	 * Exercise 3
	 */
	def countChange(money: Int, coins: List[Int]): Int = {
		if (money == 0) {
			1
		}
		else {
			if (coins.isEmpty) {
				0
			} else {
				(0 to money./(coins.head)).map(i => countChange(money - i * coins.head, coins.tail)).sum
			}
		}
	}
}
