/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.rocksdb.*;
import org.rocksdb.RocksMemEnv;
import org.rocksdb.util.SizeUnit;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  private RocksDB dbObj;

  private String dbDirectory = "/tmp";

  static {
    RocksDB.loadLibrary();
  }

  public void init() throws DBException {
    Properties props = getProperties();

    // String portString = props.getProperty(PORT_PROPERTY);

    Options options = new Options();
    try {
      prepareOptions(options);
      dbObj = RocksDB.open(options, dbDirectory);
    } catch (RocksDBException e) {
      // Convert RocksDB exception to DBException
      throw new DBException(e.getMessage());
    }
  }

  public void cleanup() throws DBException {
    if (dbObj != null) {
      dbObj.close();
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }
  private void prepareReadOptions(ReadOptions options) {
    options.setVerifyChecksums((Boolean)flags_.get(Flag.verify_checksum));
    options.setTailing((Boolean)flags_.get(Flag.use_tailing_iterator));
  }

  private void prepareWriteOptions(WriteOptions options) {
    options.setSync((Boolean)flags_.get(Flag.sync));
    options.setDisableWAL((Boolean)flags_.get(Flag.disable_wal));
  }

  private void prepareOptions(Options options) throws RocksDBException {
    if (!useExisting_) {
      options.setCreateIfMissing(true);
    } else {
      options.setCreateIfMissing(false);
    }
    if (useMemenv_) {
      options.setEnv(new RocksMemEnv());
    }
    switch (memtable_) {
      case "skip_list":
        options.setMemTableConfig(new SkipListMemTableConfig());
        break;
      case "vector":
        options.setMemTableConfig(new VectorMemTableConfig());
        break;
      case "hash_linkedlist":
        options.setMemTableConfig(
            new HashLinkedListMemTableConfig()
                .setBucketCount(hashBucketCount_));
        options.useFixedLengthPrefixExtractor(prefixSize_);
        break;
      case "hash_skiplist":
      case "prefix_hash":
        options.setMemTableConfig(
            new HashSkipListMemTableConfig()
                .setBucketCount(hashBucketCount_));
        options.useFixedLengthPrefixExtractor(prefixSize_);
        break;
      default:
        System.err.format(
            "unable to detect the specified memtable, " +
                "use the default memtable factory %s%n",
            options.memTableFactoryName());
        break;
    }
    if (usePlainTable_) {
      options.setTableFormatConfig(
          new PlainTableConfig().setKeySize(keySize_));
    } else {
      BlockBasedTableConfig table_options = new BlockBasedTableConfig();
      table_options.setBlockSize((Long)flags_.get(Flag.block_size))
                   .setBlockCacheSize((Long)flags_.get(Flag.cache_size))
                   .setCacheNumShardBits(
                      (Integer)flags_.get(Flag.cache_numshardbits));
      options.setTableFormatConfig(table_options);
    }
    options.setWriteBufferSize(
        (Long)flags_.get(Flag.write_buffer_size));
    options.setMaxWriteBufferNumber(
        (Integer)flags_.get(Flag.max_write_buffer_number));
    options.setMaxBackgroundCompactions(
        (Integer)flags_.get(Flag.max_background_compactions));
    options.getEnv().setBackgroundThreads(
        (Integer)flags_.get(Flag.max_background_compactions));
    options.setMaxBackgroundFlushes(
        (Integer)flags_.get(Flag.max_background_flushes));
    options.setMaxOpenFiles(
        (Integer)flags_.get(Flag.open_files));
    options.setDisableDataSync(
        (Boolean)flags_.get(Flag.disable_data_sync));
    options.setUseFsync(
        (Boolean)flags_.get(Flag.use_fsync));
    options.setWalDir(
        (String)flags_.get(Flag.wal_dir));
    options.setDeleteObsoleteFilesPeriodMicros(
        (Integer)flags_.get(Flag.delete_obsolete_files_period_micros));
    options.setTableCacheNumshardbits(
        (Integer)flags_.get(Flag.table_cache_numshardbits));
    options.setAllowMmapReads(
        (Boolean)flags_.get(Flag.mmap_read));
    options.setAllowMmapWrites(
        (Boolean)flags_.get(Flag.mmap_write));
    options.setAdviseRandomOnOpen(
        (Boolean)flags_.get(Flag.advise_random_on_open));
    options.setUseAdaptiveMutex(
        (Boolean)flags_.get(Flag.use_adaptive_mutex));
    options.setBytesPerSync(
        (Long)flags_.get(Flag.bytes_per_sync));
    options.setBloomLocality(
        (Integer)flags_.get(Flag.bloom_locality));
    options.setMinWriteBufferNumberToMerge(
        (Integer)flags_.get(Flag.min_write_buffer_number_to_merge));
    options.setMemtablePrefixBloomSizeRatio((Double) flags_.get(Flag.memtable_bloom_size_ratio));
    options.setNumLevels(
        (Integer)flags_.get(Flag.num_levels));
    options.setTargetFileSizeBase(
        (Integer)flags_.get(Flag.target_file_size_base));
    options.setTargetFileSizeMultiplier(
        (Integer)flags_.get(Flag.target_file_size_multiplier));
    options.setMaxBytesForLevelBase(
        (Integer)flags_.get(Flag.max_bytes_for_level_base));
    options.setMaxBytesForLevelMultiplier(
        (Integer)flags_.get(Flag.max_bytes_for_level_multiplier));
    options.setLevelZeroStopWritesTrigger(
        (Integer)flags_.get(Flag.level0_stop_writes_trigger));
    options.setLevelZeroSlowdownWritesTrigger(
        (Integer)flags_.get(Flag.level0_slowdown_writes_trigger));
    options.setLevelZeroFileNumCompactionTrigger(
        (Integer)flags_.get(Flag.level0_file_num_compaction_trigger));
    options.setSoftRateLimit(
        (Double)flags_.get(Flag.soft_rate_limit));
    options.setHardRateLimit(
        (Double)flags_.get(Flag.hard_rate_limit));
    options.setRateLimitDelayMaxMilliseconds(
        (Integer)flags_.get(Flag.rate_limit_delay_max_milliseconds));
    options.setMaxGrandparentOverlapFactor(
        (Integer)flags_.get(Flag.max_grandparent_overlap_factor));
    options.setDisableAutoCompactions(
        (Boolean)flags_.get(Flag.disable_auto_compactions));
    options.setSourceCompactionFactor(
        (Integer)flags_.get(Flag.source_compaction_factor));
    options.setMaxSuccessiveMerges(
        (Integer)flags_.get(Flag.max_successive_merges));
    options.setWalTtlSeconds((Long)flags_.get(Flag.wal_ttl_seconds));
    options.setWalSizeLimitMB((Long)flags_.get(Flag.wal_size_limit_MB));
    if(flags_.get(Flag.java_comparator) != null) {
      options.setComparator(
          (AbstractComparator)flags_.get(Flag.java_comparator));
    }
  }

  private enum Flag {
    compression_ratio(0.5d,
        "Arrange to generate values that shrink to this fraction of\n" +
        "\ttheir original size after compression.") {
      @Override public Object parseValue(String value) {
        return Double.parseDouble(value);
      }
    },
    use_existing_db(false,
        "If true, do not destroy the existing database.  If you set this\n" +
        "\tflag and also specify a benchmark that wants a fresh database,\n" +
        "\tthat benchmark will fail.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    num(1000000,
        "Number of key/values to place in database.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    threads(1,
        "Number of concurrent threads to run.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    reads(null,
        "Number of read operations to do.  If negative, do --nums reads.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    key_size(16,
        "The size of each key in bytes.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    value_size(100,
        "The size of each value in bytes.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    write_buffer_size(4 * SizeUnit.MB,
        "Number of bytes to buffer in memtable before compacting\n" +
        "\t(initialized to default value by 'main'.)") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    max_write_buffer_number(2,
             "The number of in-memory memtables. Each memtable is of size\n" +
             "\twrite_buffer_size.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    prefix_size(0, "Controls the prefix size for HashSkipList, HashLinkedList,\n" +
                   "\tand plain table.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    keys_per_prefix(0, "Controls the average number of keys generated\n" +
             "\tper prefix, 0 means no special handling of the prefix,\n" +
             "\ti.e. use the prefix comes with the generated random number.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    memtablerep("skip_list",
        "The memtable format.  Available options are\n" +
        "\tskip_list,\n" +
        "\tvector,\n" +
        "\thash_linkedlist,\n" +
        "\thash_skiplist (prefix_hash.)") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },
    hash_bucket_count(SizeUnit.MB,
        "The number of hash buckets used in the hash-bucket-based\n" +
        "\tmemtables.  Memtables that currently support this argument are\n" +
        "\thash_linkedlist and hash_skiplist.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    writes_per_second(10000,
        "The write-rate of the background writer used in the\n" +
        "\t`readwhilewriting` benchmark.  Non-positive number indicates\n" +
        "\tusing an unbounded write-rate in `readwhilewriting` benchmark.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    use_plain_table(false,
        "Use plain-table sst format.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    cache_size(-1L,
        "Number of bytes to use as a cache of uncompressed data.\n" +
        "\tNegative means use default settings.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    seed(0L,
        "Seed base for random number generators.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    num_levels(7,
        "The total number of levels.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    numdistinct(1000,
        "Number of distinct keys to use. Used in RandomWithVerify to\n" +
        "\tread/write on fewer keys so that gets are more likely to find the\n" +
        "\tkey and puts are more likely to update the same key.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    merge_keys(-1,
        "Number of distinct keys to use for MergeRandom and\n" +
        "\tReadRandomMergeRandom.\n" +
        "\tIf negative, there will be FLAGS_num keys.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    bloom_locality(0,"Control bloom filter probes locality.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    duration(0,"Time in seconds for the random-ops tests to run.\n" +
        "\tWhen 0 then num & reads determine the test duration.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    num_multi_db(0,
        "Number of DBs used in the benchmark. 0 means single DB.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    histogram(false,"Print histogram of operation timings.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    min_write_buffer_number_to_merge(
        defaultOptions_.minWriteBufferNumberToMerge(),
        "The minimum number of write buffers that will be merged together\n" +
        "\tbefore writing to storage. This is cheap because it is an\n" +
        "\tin-memory merge. If this feature is not enabled, then all these\n" +
        "\twrite buffers are flushed to L0 as separate files and this\n" +
        "\tincreases read amplification because a get request has to check\n" +
        "\tin all of these files. Also, an in-memory merge may result in\n" +
        "\twriting less data to storage if there are duplicate records\n" +
        "\tin each of these individual write buffers.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    max_background_compactions(
        defaultOptions_.maxBackgroundCompactions(),
        "The maximum number of concurrent background compactions\n" +
        "\tthat can occur in parallel.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    max_background_flushes(
        defaultOptions_.maxBackgroundFlushes(),
        "The maximum number of concurrent background flushes\n" +
        "\tthat can occur in parallel.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    /* TODO(yhchiang): enable the following
    compaction_style((int32_t) defaultOptions_.compactionStyle(),
        "style of compaction: level-based vs universal.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },*/
    universal_size_ratio(0,
        "Percentage flexibility while comparing file size\n" +
        "\t(for universal compaction only).") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    universal_min_merge_width(0,"The minimum number of files in a\n" +
        "\tsingle compaction run (for universal compaction only).") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    universal_max_merge_width(0,"The max number of files to compact\n" +
        "\tin universal style compaction.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    universal_max_size_amplification_percent(0,
        "The max size amplification for universal style compaction.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    universal_compression_size_percent(-1,
        "The percentage of the database to compress for universal\n" +
        "\tcompaction. -1 means compress everything.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    block_size(defaultBlockBasedTableOptions_.blockSize(),
        "Number of bytes in a block.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    compressed_cache_size(-1,
        "Number of bytes to use as a cache of compressed data.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    open_files(defaultOptions_.maxOpenFiles(),
        "Maximum number of files to keep open at the same time\n" +
        "\t(use default if == 0)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    bloom_bits(-1,"Bloom filter bits per key. Negative means\n" +
        "\tuse default settings.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    memtable_bloom_size_ratio(0, "Ratio of memtable used by the bloom filter.\n"
            + "\t0 means no bloom filter.") {
      @Override public Object parseValue(String value) {
        return Double.parseDouble(value);
      }
    },
    cache_numshardbits(-1,"Number of shards for the block cache\n" +
        "\tis 2 ** cache_numshardbits. Negative means use default settings.\n" +
        "\tThis is applied only if FLAGS_cache_size is non-negative.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    verify_checksum(false,"Verify checksum for every block read\n" +
        "\tfrom storage.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    statistics(false,"Database statistics.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    writes(-1,"Number of write operations to do. If negative, do\n" +
        "\t--num reads.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    sync(false,"Sync all writes to disk.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    disable_data_sync(false,"If true, do not wait until data is\n" +
        "\tsynced to disk.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    use_fsync(false,"If true, issue fsync instead of fdatasync.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    disable_wal(false,"If true, do not write WAL for write.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    wal_dir("", "If not empty, use the given dir for WAL.") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },
    target_file_size_base(2 * 1048576,"Target file size at level-1") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    target_file_size_multiplier(1,
        "A multiplier to compute target level-N file size (N >= 2)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    max_bytes_for_level_base(10 * 1048576,
      "Max bytes for level-1") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    max_bytes_for_level_multiplier(10,
        "A multiplier to compute max bytes for level-N (N >= 2)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    level0_stop_writes_trigger(12,"Number of files in level-0\n" +
        "\tthat will trigger put stop.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    level0_slowdown_writes_trigger(8,"Number of files in level-0\n" +
        "\tthat will slow down writes.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    level0_file_num_compaction_trigger(4,"Number of files in level-0\n" +
        "\twhen compactions start.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    readwritepercent(90,"Ratio of reads to reads/writes (expressed\n" +
        "\tas percentage) for the ReadRandomWriteRandom workload. The\n" +
        "\tdefault value 90 means 90% operations out of all reads and writes\n" +
        "\toperations are reads. In other words, 9 gets for every 1 put.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    mergereadpercent(70,"Ratio of merges to merges&reads (expressed\n" +
        "\tas percentage) for the ReadRandomMergeRandom workload. The\n" +
        "\tdefault value 70 means 70% out of all read and merge operations\n" +
        "\tare merges. In other words, 7 merges for every 3 gets.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    deletepercent(2,"Percentage of deletes out of reads/writes/\n" +
        "\tdeletes (used in RandomWithVerify only). RandomWithVerify\n" +
        "\tcalculates writepercent as (100 - FLAGS_readwritepercent -\n" +
        "\tdeletepercent), so deletepercent must be smaller than (100 -\n" +
        "\tFLAGS_readwritepercent)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    delete_obsolete_files_period_micros(0,"Option to delete\n" +
        "\tobsolete files periodically. 0 means that obsolete files are\n" +
        "\tdeleted after every compaction run.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    compression_type("snappy",
        "Algorithm used to compress the database.") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },
    compression_level(-1,
        "Compression level. For zlib this should be -1 for the\n" +
        "\tdefault level, or between 0 and 9.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    min_level_to_compress(-1,"If non-negative, compression starts\n" +
        "\tfrom this level. Levels with number < min_level_to_compress are\n" +
        "\tnot compressed. Otherwise, apply compression_type to\n" +
        "\tall levels.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    table_cache_numshardbits(4,"") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    stats_interval(0,"Stats are reported every N operations when\n" +
        "\tthis is greater than zero. When 0 the interval grows over time.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    stats_per_interval(0,"Reports additional stats per interval when\n" +
        "\tthis is greater than 0.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    perf_level(0,"Level of perf collection.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    soft_rate_limit(0.0,"") {
      @Override public Object parseValue(String value) {
        return Double.parseDouble(value);
      }
    },
    hard_rate_limit(0.0,"When not equal to 0 this make threads\n" +
        "\tsleep at each stats reporting interval until the compaction\n" +
        "\tscore for all levels is less than or equal to this value.") {
      @Override public Object parseValue(String value) {
        return Double.parseDouble(value);
      }
    },
    rate_limit_delay_max_milliseconds(1000,
        "When hard_rate_limit is set then this is the max time a put will\n" +
        "\tbe stalled.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    max_grandparent_overlap_factor(10,"Control maximum bytes of\n" +
        "\toverlaps in grandparent (i.e., level+2) before we stop building a\n" +
        "\tsingle file in a level->level+1 compaction.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    readonly(false,"Run read only benchmarks.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    disable_auto_compactions(false,"Do not auto trigger compactions.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    source_compaction_factor(1,"Cap the size of data in level-K for\n" +
        "\ta compaction run that compacts Level-K with Level-(K+1) (for\n" +
        "\tK >= 1)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    wal_ttl_seconds(0L,"Set the TTL for the WAL Files in seconds.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    wal_size_limit_MB(0L,"Set the size limit for the WAL Files\n" +
        "\tin MB.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    /* TODO(yhchiang): enable the following
    bufferedio(rocksdb::EnvOptions().use_os_buffer,
        "Allow buffered io using OS buffers.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    */
    mmap_read(false,
        "Allow reads to occur via mmap-ing files.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    mmap_write(false,
        "Allow writes to occur via mmap-ing files.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    advise_random_on_open(defaultOptions_.adviseRandomOnOpen(),
        "Advise random access on table file open.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    compaction_fadvice("NORMAL",
      "Access pattern advice when a file is compacted.") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },
    use_tailing_iterator(false,
        "Use tailing iterator to access a series of keys instead of get.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    use_adaptive_mutex(defaultOptions_.useAdaptiveMutex(),
        "Use adaptive mutex.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    bytes_per_sync(defaultOptions_.bytesPerSync(),
        "Allows OS to incrementally sync files to disk while they are\n" +
        "\tbeing written, in the background. Issue one request for every\n" +
        "\tbytes_per_sync written. 0 turns it off.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },
    filter_deletes(false," On true, deletes use bloom-filter and drop\n" +
        "\tthe delete if key not present.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    },
    max_successive_merges(0,"Maximum number of successive merge\n" +
        "\toperations on a key in the memtable.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },
    db(getTempDir("rocksdb-jni"),
       "Use the db with the following name.") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },
    use_mem_env(false, "Use RocksMemEnv instead of default filesystem based\n" +
        "environment.") {
      @Override public Object parseValue(String value) {
        return parseBoolean(value);
      }
    };

    private Flag(Object defaultValue, String desc) {
      defaultValue_ = defaultValue;
      desc_ = desc;
    }

    public boolean parseBoolean(String value) {
      if (value.equals("1")) {
        return true;
      } else if (value.equals("0")) {
        return false;
      }
      return Boolean.parseBoolean(value);
    }
  }


}
