package tech.mlsql.plugins.sas

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.sas.Transpose
import tech.mlsql.version.VersionCompatibility


/**
 * 9/5/24 naopyani(naopyani@gmail.com)
 */
class TransposeApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("SASTranspose", classOf[Transpose].getName)
  }

  override def supportedVersions: Seq[String] = {
    TransposeApp.versions
  }

}

object TransposeApp extends Logging {
  private val versions = Seq(">=2.0.1")

}

