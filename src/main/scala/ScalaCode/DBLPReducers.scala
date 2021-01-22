package ScalaCode

import java.lang

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleOutputs
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.control.Breaks._
import scala.collection.mutable

object DBLPReducers {

  // Count total occurrences of each author at each venue
  class AuthorPerVenueReducer extends Reducer[Text, Text, Text, Text] {
    val logger: Logger = Logger("AuthorPerVenueReducer")

    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      logger.debug(s"Executing reducer to display total count of authors for venue ${key.toString}")
      try {
        // Calculate frequency of each value (author) for a key (venue)
        val map = values.asScala.groupBy(i => i.toString).view.mapValues(_.size)
        // Sort by frequency and select top 10
        val top10 = ListMap(map.toSeq.sortWith(_._2 > _._2): _*).take(10)
        logger.debug("map : " + map)
        logger.debug("top10 : " + top10)
        // Write these authors to context
        top10 foreach { case (author, count) => context.write(key, new Text(author)) }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Reducer execution completed")
    }
  }


  class PublishedInYearReducer extends Reducer[Text, IntWritable, Text, NullWritable] {
    val logger: Logger = Logger("PublishedInYearReducer")

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, NullWritable]#Context): Unit = {
      logger.debug(s"Executing reducer to get authors and years they published for venue ${key.toString}")
      try {
        // Calculate frequency of each value (author) for a key (venue)
        val yearList = values.asScala.map(_.get()).toList.distinct.sorted
        breakable {
          for (x <- yearList.sliding(10).filter(_.size == 10).toList) {
            if ((x.last - x.head + 1) == 10) {
              context.write(key, NullWritable.get())
              break
            }
          }
        }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Reducer execution completed")
    }
  }


  class PublicationWithOneAuthorReducer extends Reducer[Text, Text, Text, Text] {
    val logger: Logger = Logger("PublicationWithOneAuthorMapper")

    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      logger.debug(s"Executing reducer to display publications with only one author for venue ${key.toString}")
      try {
        // Write venue and publisher to context
        values.asScala foreach { value => context.write(key, value) }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Reducer execution completed")
    }
  }


  class PublicationWithMaxAuthorsReducer extends Reducer[Text, Text, Text, Text] {
    val logger: Logger = Logger("PublicationWithMaxAuthorsReducer")

    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      logger.debug(s"Executing reducer to display publications with max authors for venue ${key.toString}")
      try {
        // Write venue and publisher to context
        val valuesList = values.asScala.map(_.toString.split(",")).toList
        val maxSize = valuesList.maxBy(_.size).size
        val publicationList = valuesList.filter(_.size == maxSize).map(_ (0))
        publicationList foreach (publication => context.write(key, new Text(publication)))
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Reducer execution completed")
    }
  }


  class CoAuthorsReducer extends Reducer[Text, Text, Text, NullWritable] {
    val logger: Logger = Logger("CoAuthorsReducer")
    val numberOfCoAuthors = mutable.Map[String,Int]()
//    val mos = MultipleOutputs

    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, NullWritable]#Context): Unit = {
      logger.debug(s"Executing reducer to get authors who published with max co-authors.")
      try {
        // get number of distinct co-authors
        val valuesList = values.asScala.map(_.toString).toList.distinct.filter(_.size > 0)
        numberOfCoAuthors.addOne(key.toString, valuesList.size)
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Reducer execution completed")
    }

    override def cleanup(context: Reducer[Text, Text, Text, NullWritable]#Context): Unit = {
      val sortedList = ListMap(numberOfCoAuthors.toSeq.sortWith(_._2 > _._2):_*)
      logger.debug("Authors with number of distinct co-authors sortedList : " + sortedList)
      val top100 = sortedList.take(100)
      top100.map(author => context.write(new Text(author._1),NullWritable.get()))
      context.write(new Text("********************************************"),NullWritable.get())
      val bottom100 = sortedList.takeRight(100)
      bottom100.map(author => context.write(new Text(author._1),NullWritable.get()))
    }
  }


}
