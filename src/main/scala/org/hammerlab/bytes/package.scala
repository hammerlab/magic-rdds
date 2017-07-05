package org.hammerlab

package object bytes {
  /**
   * Enables syntax like `32.MB` or `32 MB` for constructing [[Bytes]]
   */
  implicit class BytesWrapper(val value: Int)
    extends AnyVal {
    def  B: Bytes = new  B(value)
    def KB: Bytes = new KB(value)
    def MB: Bytes = new MB(value)
    def GB: Bytes = new GB(value)
    def TB: Bytes = new TB(value)
    def PB: Bytes = new PB(value)
    def EB: Bytes = new EB(value)
  }
}
