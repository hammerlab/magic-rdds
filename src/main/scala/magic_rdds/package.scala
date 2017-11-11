
package object magic_rdds
  extends size  /** must come first because extends [[org.hammerlab.magic.rdd.cache.RDDCache]] class */
     with Serializable
     with batch
     with cmp
     with collect
     with fold
     with keyed
     with ordered
     with partitions
     with prefix_sum
     with rev
     with run_length
     with sample
     with scan
     with sliding
     with sort
     with zip
