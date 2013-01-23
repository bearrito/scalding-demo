package barrett.rf.clustering


import com.twitter.scalding._

import cascading.tuple.Fields
import com.twitter.scalding.Tsv
import com.twitter.scalding.TextLine

import scala.util.Random._
import scala.annotation.tailrec


/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/20/13
 * Time: 9:43 PM
 * To change this template use File | Settings | File Templates.
 */





case class PowerRecord (d : String, gap : Double, grp : Double, v : Double,  gi : Double, s1 : Double,s2 : Double,s3 : Double){
  def powerRecord2List = {
    List((this.gap),(this.grp) , (this.v) , (this.gi) ,(this.s1), this.s2,this.s3)
  }
  def diffs(that : PowerRecord) : List[Double]   ={
    this.powerRecord2List.zip(that.powerRecord2List).map(t=> t._1 - t._2)
  }
  def diffsSquared(that : PowerRecord) : List[Double] = {
    this.diffs(that).map(d => d * d)
  }
  def euclideanDistance(that : PowerRecord) : Double = {
    scala.math.sqrt(this.diffsSquared(that).sum)
  }

  def closestCentroid(cs : Centroids) : Centroid = cs.means.map(c => (c,this.euclideanDistance(c.pr))).minBy(t => t._2)._1
}
object PowerRecord {


  def fromList(l : List[Double])  = PowerRecord("",l(0),l(1),l(2),l(3),l(4),l(5),l(6))


}

case class Centroid(pr : PowerRecord)
case class Centroids(means : List[Centroid])
object Centroids {

  def  append(cs : Centroids, centroid : Centroid) : Centroids = Centroids( centroid :: cs.means)


}

case class PartitionedPowerRecord(pr : PowerRecord, centroid : Centroid, converged : Boolean)
object PartitionedPowerRecord {


  def apply(pr : PowerRecord, centroid : Centroid) = new PartitionedPowerRecord(pr,centroid,false)
  def apply(t : (PartitionedPowerRecord, Centroids)) = {

    val ppr = t._1
    val pr = ppr.pr
    val newClosest =   pr.closestCentroid(t._2)
    val stable =   ppr.centroid == newClosest
    new PartitionedPowerRecord(pr,newClosest,stable )
  }



}

case class AggregratingCentroid(count : Int,mean : List[Double]){

  def +(that : PartitionedPowerRecord) : AggregratingCentroid = this.+(that.pr)
  def +(that : PowerRecord) : AggregratingCentroid   = {
    this.mean match
    {
      case h::t => {

        val mean = this.mean.zip(that.powerRecord2List).map(t => (t._1 + t._2)/2)
        AggregratingCentroid(this.count + 1, mean)
      }
        case Nil => AggregratingCentroid(this.count + 1, that.powerRecord2List)
      }


    }




  lazy val evaluate :  Centroid = {

    val c = Centroid(PowerRecord.fromList(this.mean))
    c

  }

}
case class Wrapper(id : Int, pr : PowerRecord)

object Wrapper{

  implicit def wrapper2PowerRecord(w : Wrapper)  : PowerRecord = w.pr


}
object Clustering{
  def line2PowerRecord(line : String) : Option[PowerRecord]  = {
    val splits = line.split(';')
    val isDirtyData = splits.contains("?")
    isDirtyData match {
      case false => {

        val day :String  = splits(0)
        val time : String = splits(1)
        val date :String = day + time
        val gap = splits(2).toDouble
        val grp = splits(3).toDouble
        val v = splits(4).toDouble
        val gi = splits(5).toDouble
        val s1 = splits(6).toDouble
        val s2 = splits(7).toDouble
        val s3 = splits(8).toDouble
        Some(PowerRecord(date,gap,grp,v,gi,s1,s2,s3))


      }
      case true=> None


    }
  }
  def shouldKeep(pr : Option[PowerRecord]) : Boolean = {
    pr match {
    case Some(pr)  => true
    case None => false
    }
  }
}


class Clustering (args : Args) extends Job(args)
{
  val STEPS : Int = args.getOrElse("steps","4").toInt
  def cluster() = {

     println("init")
     val ppr = init()
     val result = cluster0(STEPS)(ppr,false)
     val centroids = result.map(ppr => ppr.centroid)
     result.write(Tsv(args("output") + "/result"))


  }
  private def init() :  TypedPipe[PartitionedPowerRecord] = {
    val text = TextLine( args("input") + "/hpc.txt" )
    val typedText : TypedPipe[String] = TypedPipe.from(text)


    val powerRecords = typedText.map(line => Clustering.line2PowerRecord(line))
    val filteredPowerRecords = powerRecords.filter(pr => Clustering.shouldKeep(pr))
      .map(opr => opr.get )

    val initialCentroidText =  TextLine( args("input") + "/initialCentroids.txt" )
    val initialCentroidsTyped : TypedPipe[String] =  TypedPipe.from(initialCentroidText)

    val initialCentroids   =   initialCentroidsTyped.map(line => Clustering.line2PowerRecord(line))
      .filter(pr => Clustering.shouldKeep(pr))
      .map(opr => opr.get)
      .map(pr => Centroid(pr))
      .groupAll.foldLeft(Centroids(List.empty[Centroid]))((cs,c) => Centroids.append(cs,c)).map(t => t._2)

    val grp = filteredPowerRecords.cross(initialCentroids)
    val partitions = grp.map(t => PartitionedPowerRecord(t._1,t._1.closestCentroid(t._2)) )
    partitions


  }
  @tailrec
  final def cluster0(steps : Int)(pipe : TypedPipe[PartitionedPowerRecord], converged : Boolean) : TypedPipe[PartitionedPowerRecord] = {
      if (steps <= 0 || converged)

      {
        pipe.write(Tsv(args("output") + "/debug/exit-pipe" + steps)).forceToDisk
      }
     else
      {
        val l = List.empty[Double]
        val newCentroids = pipe.groupBy(ppr => ppr.centroid.hashCode()).foldLeft(AggregratingCentroid(0,l))((ac,ppr) => ac.+(ppr))
          .map(t => t._2.evaluate).groupAll.foldLeft(Centroids(List.empty[Centroid]))((cs,c) => Centroids.append(cs,c))

        newCentroids.write(Tsv(args("output") + "/debug/centroid-pass" + steps)).forceToDisk

        val partitions = pipe.cross(newCentroids)
          .map(t => (t._1,t._2._2) )
          .map(t => PartitionedPowerRecord(t) ).forceToDisk


        //val converged = partitions.filter(ppr => ppr.converged == false)
         partitions.write(Tsv(args("output") + "/debug/partitions-pass" + steps)).forceToDisk
        cluster0(steps -1)(partitions,false)

      }



}

   cluster()






}












