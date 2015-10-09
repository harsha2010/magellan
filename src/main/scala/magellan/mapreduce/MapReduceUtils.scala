/**
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package magellan.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.TaskAttemptContext

private[magellan] object MapReduceUtils {
  def getConfigurationFromContext(context: TaskAttemptContext): Configuration = {
    // Use reflection to get the Configuration. This is necessary because TaskAttemptContext
    // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
    val method = context.getClass.getMethod("getConfiguration")
    method.invoke(context).asInstanceOf[Configuration]
  }
}
