package ScalaCode

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.xml.XML

/**
 * Definition of all the mapper functions required for different tasks.
 */

object DBLPMappers {
  val logger: Logger = Logger("ParseXML")

  // Utility function used by mappers to extract elements from XML block
  def getElement(xmlString: String,tag: String): String = {
    val xmlElement = XML.loadString(xmlString)
    logger.debug("xmlElement : " + xmlElement.toString )
    if (tag == "venue") {
      xmlElement.child(0).label match {
        case "article" => (xmlElement \\ "journal").text.toLowerCase.trim
        case "inproceedings" | "proceedings" | "incollection" => (xmlElement \\ "booktitle").text.toLowerCase.trim
        case "book" | "phdthesis" | "mastersthesis" => (xmlElement \\ "publisher").text.toLowerCase.trim
        case _ => "Invalid"
      }
    }
    else if (tag == "author") (xmlElement \\ "author").map(author => author.text.toLowerCase.trim).mkString(",")
    else if (tag == "year") (xmlElement \\ "year").text.trim
    else if (tag == "publication") (xmlElement \\ "title").text.toLowerCase.trim
    else ""
  }

  // Mapper to find top authors for each venue
  class AuthorPerVenueMapper extends Mapper[LongWritable, Text, Text, Text] {
    val logger: Logger = Logger("AuthorPerVenueMapper")
    val dtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug("Executing mapper to get count of publications of each author")
      try {
        val xmlString =
          s"""<?xml version="1.0" encoding="ISO-8859-1"?>
                          <!DOCTYPE dblp SYSTEM "$dtd">
                          <dblp>""" + value.toString + "</dblp>"
        val venue = getElement(xmlString, "venue")
        val authors = getElement(xmlString, "author").split(",")
        if (venue.length > 0) {
          authors foreach (author => {
            logger.debug("writing to context (" + venue + ", " + author + ")")
            if (author.length > 0) context.write(new Text(venue), new Text(author))
          })
        }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Mapper execution completed")
    }
  }

  // Mapper to get authors who published for at least 10 consecutive years
  class PublishedInYearMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    val logger: Logger = Logger("PublishedInYearMapper")
    val dtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      logger.debug("Executing mapper to get authors and years they published")
      try {
        val xmlString =
          s"""<?xml version="1.0" encoding="ISO-8859-1"?>
                          <!DOCTYPE dblp SYSTEM "$dtd">
                          <dblp>""" + value.toString + "</dblp>"
        val year = getElement(xmlString, "year")
        val authors = getElement(xmlString, "author").split(",")
        authors foreach (author => context.write(new Text(author), new IntWritable(year.toInt)))
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Mapper execution completed")
    }
  }


  class PublicationWithOneAuthorMapper extends Mapper[LongWritable, Text, Text, Text] {
    val logger: Logger = Logger("PublicationWithOneAuthorMapper")
    val dtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug("Executing mapper to display publications with only one author")
      try {
        val xmlString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
                            <!DOCTYPE dblp SYSTEM "$dtd">
                            <dblp>""" + value.toString + "</dblp>"
        val venue = getElement(xmlString,"venue")
        if (venue.length>0 && getElement(xmlString,"author").split(",").size == 1) {
          context.write(new Text(venue), new Text(getElement(xmlString,"publication")))
        }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Mapper execution completed")
    }
  }


  class PublicationWithMaxAuthorsMapper extends Mapper[LongWritable, Text, Text, Text] {
    val logger: Logger = Logger("PublicationWithMaxAuthorsMapper")
    val dtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug("Executing mapper to display publications with max authors")
      try {
        val xmlString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
                            <!DOCTYPE dblp SYSTEM "$dtd">
                            <dblp>""" + value.toString + "</dblp>"
        val venue = getElement(xmlString,"venue")
        val publication = getElement(xmlString,"publication")
        if (venue.length>0 && publication.length>0) {
          val authors = getElement(xmlString,"author")
          context.write(new Text(venue), new Text(publication + "," + authors))
        }
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Mapper execution completed")
    }
  }


  class CoAuthorsMapper extends Mapper[LongWritable, Text, Text, Text] {
    val logger: Logger = Logger("CoAuthorsMapper")
    val dtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug("Executing mapper to get co-authors")
      try {
        val xmlString =
          s"""<?xml version="1.0" encoding="ISO-8859-1"?>
                          <!DOCTYPE dblp SYSTEM "$dtd">
                          <dblp>""" + value.toString + "</dblp>"
        val authors = getElement(xmlString, "author").split(",")
        if (authors.size > 1) {
          val pairs = authors.combinations(2).toList
          pairs foreach { pair =>
            if (pair(0).length > 0 && pair(1).length > 0) {
              logger.debug("writing pair (" + pair(0) + "," + pair(1) + ")")
              context.write(new Text(pair(0)), new Text(pair(1)))
              logger.debug("writing pair (" + pair(1) + "," + pair(0) + ")")
              context.write(new Text(pair(1)), new Text(pair(0)))
            }
          }
        }
        else if (authors.size == 1 && authors(0).length > 0) {
          logger.debug("only one author found : (" + authors(0) + ",'')")
          context.write(new Text(authors(0)), new Text(""))
        }
        else logger.debug("No Authors to send")
      }
      catch {
        case e : Exception => logger.warn("Exception! "+e.printStackTrace)
      }
      logger.debug("Mapper execution completed")
    }
  }

}