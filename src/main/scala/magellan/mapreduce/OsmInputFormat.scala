package magellan.mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import magellan.io.{OsmKey, OsmShape}

class OsmInputFormat extends FileInputFormat[OsmKey, OsmShape] {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext) : RecordReader[OsmKey, OsmShape] = {
    new OsmRecordReader
  }
  
  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}
