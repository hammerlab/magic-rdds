
import org.hammerlab.magic.rdd

package object magic_rdds
  extends rdd.size.Size
    with rdd.sliding.ops
    with rdd.sort.ops {
  object size extends rdd.size.Size
  object sliding extends rdd.sliding.ops
  object sort extends rdd.sort.ops
}
