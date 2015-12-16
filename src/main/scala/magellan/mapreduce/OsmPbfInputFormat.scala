package magellan.mapreduce

import magellan.io.{OsmKey, OsmShape}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

class OsmPbfInputFormat extends FileInputFormat[OsmKey, OsmShape] {

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[OsmKey, OsmShape] = {
    new OsmPbfRecordReader
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}
