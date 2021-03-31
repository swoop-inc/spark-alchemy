package org.apache.spark.sql.test

import java.io.File

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

trait SQLHelper {

  /**
    * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
    * configurations.
    */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.staticConfKeys.contains(k)) {
        throw new AnalysisException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  /**
    * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
    * a file/directory is created there by `f`, it will be delete after `f` returns.
    */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }
}
