package kmeans

import java.util.concurrent._

import scala.collection.{Map, Seq, mutable}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.collection.parallel.CollectionConverters._
import scala.math._
import org.junit._
import org.junit.Assert.assertEquals

import scala.collection.parallel.mutable.
ParArray

class KMeansSuite {

  object KM extends KMeans

  import KM._

  def checkClassify(points: Seq[Point], means: Seq[Point], expected: Map[Point, Seq[Point]]): Unit =
    assertEquals(expected, classify(points, means))

  @Test def `'classify should work for empty 'points' and empty 'means'`: Unit = {
    val points: Seq[Point] = IndexedSeq()
    val means: Seq[Point] = IndexedSeq()
    val expected = Map[Point, Seq[Point]]()
    checkClassify(points, means, expected)
  }

  @Test def `'classify' should work for empty 'points' and 'means' == Seq(Point(1,1,1))`: Unit = {
    val points: Seq[Point] = IndexedSeq()
    val mean = new Point(1, 1, 1)
    val means: Seq[Point] = IndexedSeq(mean)
    val expected = Map[Point, Seq[Point]]((mean, Seq()))
    checkClassify(points, means, expected)
  }

  @Test def `'classify' should work for 'points' == Seq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == Seq((0, 0, 0))`: Unit = {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: Seq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean = new Point(0, 0, 0)
    val means: Seq[Point] = IndexedSeq(mean)
    val expected = Map((mean, Seq(p1, p2, p3, p4)))
    checkClassify(points, means, expected)
  }

  @Test def `'classify' should work for 'points' == Seq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == Seq((1, 0, 0), (-1, 0, 0))`: Unit = {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: Seq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean1 = new Point(1, 0, 0)
    val mean2 = new Point(-1, 0, 0)
    val means: Seq[Point] = IndexedSeq(mean1, mean2)
    val expected = Map((mean1, Seq(p1, p2)), (mean2, Seq(p3, p4)))
    checkClassify(points, means, expected)
  }

  def checkParClassify(points: ParSeq[Point], means: ParSeq[Point], expected: ParMap[Point, ParSeq[Point]]): Unit = {
    assertEquals(s"classify($points, $means) should equal to $expected", expected, classify(points, means))
  }

  @Test def `'classify' with data parallelism should work for empty 'points' and empty 'means'`: Unit = {
    val points: ParSeq[Point] = IndexedSeq().par
    val means: ParSeq[Point] = IndexedSeq().par
    val expected = ParMap[Point, ParSeq[Point]]()
    checkParClassify(points, means, expected)
  }

  @Test def `'covered' should return 'false'`: Unit = {
    assert(!converged(
      0.001,
      ParArray(Point(1.0, 1.0, 1.0), Point(2.0, 2.0, 2.0), Point(3.0, 3.0, 3.0), Point(4.0, 4.0, 4.0), Point(5.0, 5.0, 5.0), Point(6.0, 6.0, 6.0), Point(7.0, 7.0, 7.0), Point(8.0, 8.0, 8.0), Point(9.0, 9.0, 9.0), Point(10.0, 10.0, 10.0), Point(11.0, 11.0, 11.0), Point(12.0, 12.0, 12.0), Point(13.0, 13.0, 13.0), Point(14.0, 14.0, 14.0), Point(15.0, 15.0, 15.0), Point(16.0, 16.0, 16.0), Point(17.0, 17.0, 17.0), Point(18.0, 18.0, 18.0), Point(19.0, 19.0, 19.0), Point(20.0, 20.0, 20.0), Point(21.0, 21.0, 21.0), Point(22.0, 22.0, 22.0), Point(23.0, 23.0, 23.0), Point(24.0, 24.0, 24.0), Point(25.0, 25.0, 25.0), Point(26.0, 26.0, 26.0), Point(27.0, 27.0, 27.0), Point(28.0, 28.0, 28.0), Point(29.0, 29.0, 29.0), Point(30.0, 30.0, 30.0), Point(31.0, 31.0, 31.0), Point(32.0, 32.0, 32.0), Point(33.0, 33.0, 33.0), Point(34.0, 34.0, 34.0), Point(35.0, 35.0, 35.0), Point(36.0, 36.0, 36.0), Point(37.0, 37.0, 37.0), Point(38.0, 38.0, 38.0), Point(39.0, 39.0, 39.0), Point(40.0, 40.0, 40.0), Point(41.0, 41.0, 41.0), Point(42.0, 42.0, 42.0), Point(43.0, 43.0, 43.0), Point(44.0, 44.0, 44.0), Point(45.0, 45.0, 45.0), Point(46.0, 46.0, 46.0), Point(47.0, 47.0, 47.0), Point(48.0, 48.0, 48.0), Point(49.0, 49.0, 49.0), Point(50.0, 50.0, 50.0), Point(51.0, 51.0, 51.0), Point(52.0, 52.0, 52.0), Point(53.0, 53.0, 53.0), Point(54.0, 54.0, 54.0), Point(55.0, 55.0, 55.0), Point(56.0, 56.0, 56.0), Point(57.0, 57.0, 57.0), Point(58.0, 58.0, 58.0), Point(59.0, 59.0, 59.0), Point(60.0, 60.0, 60.0), Point(61.0, 61.0, 61.0), Point(62.0, 62.0, 62.0), Point(63.0, 63.0, 63.0), Point(64.0, 64.0, 64.0), Point(65.0, 65.0, 65.0), Point(66.0, 66.0, 66.0), Point(67.0, 67.0, 67.0), Point(68.0, 68.0, 68.0), Point(69.0, 69.0, 69.0), Point(70.0, 70.0, 70.0), Point(71.0, 71.0, 71.0), Point(72.0, 72.0, 72.0), Point(73.0, 73.0, 73.0), Point(74.0, 74.0, 74.0), Point(75.0, 75.0, 75.0), Point(76.0, 76.0, 76.0), Point(77.0, 77.0, 77.0), Point(78.0, 78.0, 78.0), Point(79.0, 79.0, 79.0), Point(80.0, 80.0, 80.0), Point(81.0, 81.0, 81.0), Point(82.0, 82.0, 82.0), Point(83.0, 83.0, 83.0), Point(84.0, 84.0, 84.0), Point(85.0, 85.0, 85.0), Point(86.0, 86.0, 86.0), Point(87.0, 87.0, 87.0), Point(88.0, 88.0, 88.0), Point(89.0, 89.0, 89.0), Point(90.0, 90.0, 90.0), Point(91.0, 91.0, 91.0), Point(92.0, 92.0, 92.0), Point(93.0, 93.0, 93.0), Point(94.0, 94.0, 94.0), Point(95.0, 95.0, 95.0), Point(96.0, 96.0, 96.0), Point(97.0, 97.0, 97.0), Point(98.0, 98.0, 98.0), Point(99.0, 99.0, 99.0)),
      ParArray(Point(1.0, 1.0, 1.0), Point(2.0, 2.0, 2.0), Point(3.0, 3.0, 3.0), Point(4.0, 4.0, 4.0), Point(5.0, 5.0, 5.0), Point(6.0, 6.0, 6.0), Point(7.0, 7.0, 7.0), Point(8.0, 8.0, 8.0), Point(9.0, 9.0, 9.0), Point(10.0, 10.0, 10.0), Point(11.0, 11.0, 11.0), Point(12.0, 12.0, 12.0), Point(13.0, 13.0, 13.0), Point(14.0, 14.0, 14.0), Point(15.0, 15.0, 15.0), Point(16.0, 16.0, 16.0), Point(17.0, 17.0, 17.0), Point(18.0, 18.0, 18.0), Point(19.0, 19.0, 19.0), Point(20.0, 20.0, 20.0), Point(21.0, 21.0, 21.0), Point(22.0, 22.0, 22.0), Point(23.0, 23.0, 23.0), Point(24.0, 24.0, 24.0), Point(25.0, 25.0, 25.0), Point(26.0, 26.0, 26.0), Point(27.0, 27.0, 27.0), Point(28.0, 28.0, 28.0), Point(29.0, 29.0, 29.0), Point(30.0, 30.0, 30.0), Point(31.0, 31.0, 31.0), Point(32.0, 32.0, 32.0), Point(33.0, 33.0, 33.0), Point(34.0, 34.0, 34.0), Point(35.0, 35.0, 35.0), Point(36.0, 36.0, 36.0), Point(37.0, 37.0, 37.0), Point(38.0, 38.0, 38.0), Point(39.0, 39.0, 39.0), Point(40.0, 40.0, 40.0), Point(41.0, 41.0, 41.0), Point(42.0, 42.0, 42.0), Point(43.0, 43.0, 43.0), Point(44.0, 44.0, 44.0), Point(45.0, 45.0, 45.0), Point(46.0, 46.0, 46.0), Point(47.0, 47.0, 47.0), Point(48.0, 48.0, 48.0), Point(49.0, 49.0, 49.0), Point(50.0, 50.0, 50.0), Point(51.0, 51.0, 51.0), Point(52.0, 52.0, 52.0), Point(53.0, 53.0, 53.0), Point(54.0, 54.0, 54.0), Point(55.0, 55.0, 55.0), Point(56.0, 56.0, 56.0), Point(57.0, 57.0, 57.0), Point(58.0, 58.0, 58.0), Point(59.0, 59.0, 59.0), Point(60.0, 60.0, 60.0), Point(61.0, 61.0, 61.0), Point(62.0, 62.0, 62.0), Point(63.0, 63.0, 63.0), Point(64.0, 64.0, 64.0), Point(65.0, 65.0, 65.0), Point(66.0, 66.0, 66.0), Point(67.0, 67.0, 67.0), Point(68.0, 68.0, 68.0), Point(69.0, 69.0, 69.0), Point(70.0, 70.0, 70.0), Point(71.0, 71.0, 71.0), Point(72.0, 72.0, 72.0), Point(73.0, 73.0, 73.0), Point(74.0, 74.0, 74.0), Point(75.0, 75.0, 75.0), Point(76.0, 76.0, 76.0), Point(77.0, 77.0, 77.0), Point(78.0, 78.0, 78.0), Point(79.0, 79.0, 79.0), Point(80.0, 80.0, 80.0), Point(81.0, 81.0, 81.0), Point(82.0, 82.0, 82.0), Point(83.0, 83.0, 83.0), Point(84.0, 84.0, 84.0), Point(85.0, 85.0, 85.0), Point(86.0, 86.0, 86.0), Point(87.0, 87.0, 87.0), Point(88.0, 88.0, 88.0), Point(89.0, 89.0, 89.0), Point(90.0, 90.0, 90.0), Point(91.0, 91.0, 91.0), Point(92.0, 92.0, 92.0), Point(93.0, 93.0, 93.0), Point(94.0, 94.0, 94.0), Point(95.0, 95.0, 95.0), Point(96.0, 96.0, 96.0), Point(97.0, 97.0, 97.0), Point(98.0, 98.0, 98.0), Point(99.0, 99.0, 100.0))
    ))
  }

}


