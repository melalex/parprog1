package reductions

import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var i = 0
    var acc = 0

    while (i < chars.length) {
      if (acc < 0) return false
      else if (chars(i) == '(') acc += 1
      else if (chars(i) == ')') acc -= 1

      i += 1
    }

    acc == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @scala.annotation.tailrec
    def traverse(i: Int, until: Int, acc: Int, unOpenedParentheses: Int): (Int, Int) = {
      if (i < until) chars(i) match {
        case '(' => traverse(i + 1, until, acc + 1, unOpenedParentheses)
        case ')' if acc > 0 => traverse(i + 1, until, acc - 1, unOpenedParentheses)
        case ')' => traverse(i + 1, until, acc, unOpenedParentheses + 1)
        case _ => traverse(i + 1, until, acc, unOpenedParentheses)
      }
      else (acc, unOpenedParentheses)
    }

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2

        val ((acc1, unOpenedParentheses1), (acc2, unOpenedParentheses2)) = parallel(reduce(from, mid), reduce(mid, until))

        if (acc1 > unOpenedParentheses2) (acc1 - unOpenedParentheses2 + acc2) -> unOpenedParentheses1
        else acc2 -> (unOpenedParentheses2 - acc1 + unOpenedParentheses1)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
