package magellan.mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import magellan.io.OsmShape

class OsmInputFormat extends FileInputFormat[Tuple2[String, String], OsmShape] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) : RecordReader[Tuple2[String, String], OsmShape] = {
    new OsmRecordReader
  }
  
  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}
