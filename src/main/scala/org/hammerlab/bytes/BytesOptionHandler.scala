package org.hammerlab.bytes

import org.hammerlab.args4s.OptionHandler
import org.kohsuke.args4j.{ CmdLineParser, OptionDef }
import org.kohsuke.args4j.spi.Setter

class BytesOptionHandler(parser: CmdLineParser,
                         option: OptionDef,
                         setter: Setter[Option[Bytes]])
  extends OptionHandler[Bytes](
      parser,
      option,
      setter,
      "BYTES",
      Bytes(_)
  )
