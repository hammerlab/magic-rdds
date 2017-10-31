
package object magic_rdds
  extends size  /** must come first because extends [[org.hammerlab.magic.rdd.cache.RDDCache]] class */
     with collect
     with keyed
     with ordered
     with partitions
     with rev
     with run_length
     with sample
     with scan
     with sliding
     with sort
     with zip
