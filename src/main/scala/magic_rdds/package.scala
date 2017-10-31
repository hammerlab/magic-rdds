
package object magic_rdds
  extends size  /** must come first because extends [[org.hammerlab.magic.rdd.cache.RDDCache]] class */
    with rev
    with scan
    with sliding
    with sort
    with zip
