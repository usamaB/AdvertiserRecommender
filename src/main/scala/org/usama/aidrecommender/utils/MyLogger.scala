package org.usama.aidrecommender.utils

import org.apache.log4j.Logger

object MyLogger extends Serializable {
  @transient lazy val log = Logger.getRootLogger()
}
