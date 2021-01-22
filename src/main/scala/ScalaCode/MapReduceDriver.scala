package ScalaCode

import com.typesafe.scalalogging.Logger
import com.typesafe.config.ConfigFactory
import JavaCode.XmlInputFormatWithMultipleTags
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapred.lib.MultipleOutputs
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object MapReduceDriver {
  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger("MapReduceDriver")
    val configuration = new Configuration
    val conf = ConfigFactory.load("Application.conf")
//    configuration.set("mapred.textoutputformat.separator", ",")
    // Set start tags. Get list of start tags from config file
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))
    // Set end tags. Get list of end tags from config file
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
    val outputfile = new Path(args(1))
    outputfile.getFileSystem(configuration).delete(outputfile)

    //Job to find count of publications of each author
    logger.info("Starting Task-1 Top 10 published authors at each venue");
    {
      val job1 = Job.getInstance(configuration, "Authors per Venue")
      job1.setJarByClass(this.getClass)
      job1.setMapperClass(classOf[DBLPMappers.AuthorPerVenueMapper])
      job1.setMapOutputKeyClass(classOf[Text])
      job1.setMapOutputValueClass(classOf[Text])
      job1.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job1.setReducerClass(classOf[DBLPReducers.AuthorPerVenueReducer])
      job1.setOutputKeyClass(classOf[Text])
      job1.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job1, new Path(args(0)))
      FileOutputFormat.setOutputPath(job1, new Path(args(1) + conf.getString("AuthorPerVenueOutputPath")))
      job1.waitForCompletion(true)
    }

    //Job to find authors who published without interruption for 10 or more consecutive years
    logger.info("Starting Task-2 Authors published for more than 10 years");
    {
      val job2 = Job.getInstance(configuration, "Authors for >=10 years")
      job2.setJarByClass(this.getClass)
      job2.setMapperClass(classOf[DBLPMappers.PublishedInYearMapper])
      job2.setMapOutputKeyClass(classOf[Text])
      job2.setMapOutputValueClass(classOf[IntWritable])
      job2.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job2.setReducerClass(classOf[DBLPReducers.PublishedInYearReducer])
      job2.setOutputKeyClass(classOf[Text])
      job2.setOutputValueClass(classOf[NullWritable])
      FileInputFormat.addInputPath(job2, new Path(args(0)))
      FileOutputFormat.setOutputPath(job2, new Path(args(1) + conf.getString("PublishedInYearOutputPath")))
      job2.waitForCompletion(true)
    }

    //Job to find publishers with only one author for each venue
    logger.info("Starting Task-3 List of publications that contains only one author");
    {
      val job3 = Job.getInstance(configuration, "Publications with 1 author")
      job3.setJarByClass(this.getClass)
      job3.setMapperClass(classOf[DBLPMappers.PublicationWithOneAuthorMapper])
      job3.setMapOutputKeyClass(classOf[Text])
      job3.setMapOutputValueClass(classOf[Text])
      job3.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job3.setReducerClass(classOf[DBLPReducers.PublicationWithOneAuthorReducer])
      job3.setOutputKeyClass(classOf[Text])
      job3.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job3, new Path(args(0)))
      FileOutputFormat.setOutputPath(job3, new Path(args(1) + conf.getString("PublicationWithOneAuthorOutputPath")))
      job3.waitForCompletion(true)
    }

    //Job to find publishers with max authors for each venue
    logger.info("Starting Task-4 List of publications that contains max authors");
    {
      val job4 = Job.getInstance(configuration, "Publications with max author")
      job4.setJarByClass(this.getClass)
      job4.setMapperClass(classOf[DBLPMappers.PublicationWithMaxAuthorsMapper])
      job4.setMapOutputKeyClass(classOf[Text])
      job4.setMapOutputValueClass(classOf[Text])
      job4.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job4.setReducerClass(classOf[DBLPReducers.PublicationWithMaxAuthorsReducer])
      job4.setOutputKeyClass(classOf[Text])
      job4.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job4, new Path(args(0)))
      FileOutputFormat.setOutputPath(job4, new Path(args(1) + conf.getString("PublicationWithMaxAuthorsOutputPath")))
      job4.waitForCompletion(true)
    }

    //
    logger.info("Starting Task-5 List of authors who published with max co-authors");
    {
      val job5 = Job.getInstance(configuration, "Publications with max author")
      job5.setJarByClass(this.getClass)
      job5.setMapperClass(classOf[DBLPMappers.CoAuthorsMapper])
      job5.setMapOutputKeyClass(classOf[Text])
      job5.setMapOutputValueClass(classOf[Text])
      job5.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job5.setReducerClass(classOf[DBLPReducers.CoAuthorsReducer])
      job5.setOutputKeyClass(classOf[Text])
      job5.setOutputValueClass(classOf[NullWritable])
      FileInputFormat.addInputPath(job5, new Path(args(0)))
      FileOutputFormat.setOutputPath(job5, new Path(args(1) + conf.getString("AuthorsWithCoAuthors")))
//      MultipleOutputs.addNamedOutput(job5, "task5-1", classOf[TextOutputFormat], classOf[Text], classOf[NullWritable])
//      MultipleOutputs.addNamedOutput(job5, "task5-2", classOf[TextOutputFormat], classOf[Text], classOf[NullWritable])
      job5.waitForCompletion(true)
    }

  }
}