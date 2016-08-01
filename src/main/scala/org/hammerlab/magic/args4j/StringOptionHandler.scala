package org.hammerlab.magic.args4j

import org.kohsuke.args4j.spi.{OptionHandler, Parameters, Setter}
import org.kohsuke.args4j.{CmdLineParser, OptionDef, OptionHandlerRegistry}

/**
 * Args4J option handler that populates an [[Option[String]]].
 */
class StringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[String]])
  extends OptionHandler[Option[String]](parser, option, setter) {
  override def getDefaultMetaVariable: String = "path"

  override def parseArguments(params: Parameters): Int = {
    setter.addValue(Some(params.getParameter(0)))
    1
  }
}

object StringOptionHandler {
  OptionHandlerRegistry.getRegistry.registerHandler(classOf[Option[String]], classOf[StringOptionHandler])
}
