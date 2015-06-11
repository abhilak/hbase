/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY;
import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerResult;

/**
 * The default implementation for the HeapMemoryTuner. This will do simple checks to decide
 * whether there should be changes in the heap size of memstore/block cache. When there is no block
 * cache eviction at all but there are flushes because of global heap pressure, it will increase the
 * memstore heap size and decrease block cache size. The step value for this heap size change can be
 * specified using the config <i>hbase.regionserver.heapmemory.autotuner.step</i>. When there is no
 * memstore flushes because of heap pressure but there is block cache evictions it will increase the
 * block cache heap.
 */
@InterfaceAudience.Private
class DefaultHeapMemoryTuner implements HeapMemoryTuner {

  public static final String STEP_KEY = "hbase.regionserver.heapmemory.autotuner.step";
  public static final String MAX_ALLOWED_LOSS_KEY =
      "hbase.regionserver.heapmemory.autotuner.step.max.loss";
  public static final String MIN_EXPECTED_GAIN_KEY =
      "hbase.regionserver.heapmemory.autotuner.step.min.gain";
  public static final String SUFFICIENT_MEMORY_LEVEL_KEY =
      "hbase.regionserver.heapmemory.autotuner.sufficient.memory.level";
  public static final float DEFAULT_STEP_VALUE = 0.02f; // 2%
  // Fraction change less than this will not be considered harmful
  // TODO dynamically calculate tolerance from standard deviation of percent changes
  public static final float DEFAULT_MAX_ALLOWED_LOSS_VALUE = 0.01f; // 1%
  // Fraction change less than this will not be considered useful
  public static final float DEFAULT_MIN_EXPECTED_GAIN_VALUE = 0.005f; // 0.5%
  // If current block cache size or memstore size in use is below this level relative to memory
  // provided to it then corresponding component will be considered to have sufficient memory
  public static final float DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE = 0.5f; // 50%
  // Large constant value assigned to percent changes when its undefined
  public static final float undefinedPercentChange = 1.0f; // 100%
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  private Log LOG = LogFactory.getLog(DefaultHeapMemoryTuner.class);
  private TunerResult TUNER_RESULT = new TunerResult(true);
  private Configuration conf;
  private float step = DEFAULT_STEP_VALUE;
  private float maximumAllowedLoss = DEFAULT_MAX_ALLOWED_LOSS_VALUE;
  private float minimumExpectedGain = DEFAULT_MIN_EXPECTED_GAIN_VALUE;
  private float sufficientMemoryLevel = DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  private StepDirection prevTuneDirection = StepDirection.NEUTRAL;
  private long prevFlushCount = 0;
  private long prevEvictCount = 0;
  private long prevCacheMissCount = 0;

  @Override
  public TunerResult tune(TunerContext context) {
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    long cacheMissCount = context.getCacheMissCount();
    StepDirection newTuneDirection = StepDirection.NEUTRAL;
    String tunerLog = "";
    // We can consider memstore or block cache to be sufficient if
    // we are using only a minor fraction of what have been already provided to it
    boolean earlyMemstoreSufficientCheck = (blockedFlushCount == 0 && unblockedFlushCount == 0)
            || context.getCurMemStoreUsed() < context.getCurMemStoreSize()*sufficientMemoryLevel;
    boolean earlyBlockCacheSufficientCheck = evictCount == 0 ||
            context.getCurBlockCacheUsed() < context.getCurBlockCacheSize()*sufficientMemoryLevel;
    if (earlyMemstoreSufficientCheck && earlyBlockCacheSufficientCheck) {
      prevFlushCount = blockedFlushCount + unblockedFlushCount;
      prevEvictCount = evictCount;
      prevCacheMissCount = cacheMissCount;
      prevTuneDirection = StepDirection.NEUTRAL;
      return NO_OP_TUNER_RESULT;
    }
    float newMemstoreSize;
    float newBlockCacheSize;
    if (earlyMemstoreSufficientCheck) {
      // Increase the block cache size and corresponding decrease in memstore size
      newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
    } else if (earlyBlockCacheSufficientCheck) {
      // Increase the memstore size and corresponding decrease in block cache size
      newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
    } else {
      float percentChangeInEvictCount;
      float percentChangeInFlushes;
      float percentChangeInMisses;
      if (prevEvictCount != 0) {
        percentChangeInEvictCount  = (float)(evictCount-prevEvictCount)/(float)(prevEvictCount);
      } else {
        // current and previous are both cannot be zero, assigning large percent change
        percentChangeInEvictCount  = undefinedPercentChange;
      }
      if (prevFlushCount != 0) {
        percentChangeInFlushes = (float)(blockedFlushCount + unblockedFlushCount -
            prevFlushCount)/(float)(prevFlushCount);
      } else {
        percentChangeInFlushes = undefinedPercentChange;
      }
      if (prevCacheMissCount != 0) {
        percentChangeInMisses = (float)(cacheMissCount-prevCacheMissCount)/
            (float)(prevCacheMissCount);
      } else {
        percentChangeInMisses = undefinedPercentChange;
      }
      boolean isReverting = false;
      switch (prevTuneDirection) {
      case INCREASE_BLOCK_CACHE_SIZE:
        if (percentChangeInEvictCount > -minimumExpectedGain ||
            percentChangeInFlushes > maximumAllowedLoss) {
          // reverting previous step as it was not useful
          newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          tunerLog += "Reverting previous tuning.";
          if (percentChangeInEvictCount > -minimumExpectedGain) {
            tunerLog += " As could not decrease evctions sufficiently.";
          } else {
            tunerLog += " As number of flushes rose significantly.";
          }
          isReverting = true;
        }
        break;
      case INCREASE_MEMSTORE_SIZE:
        if (percentChangeInEvictCount > maximumAllowedLoss ||
            percentChangeInFlushes > -minimumExpectedGain) {
          // reverting previous step as it was not useful
          newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          tunerLog += "Reverting previous tuning.";
          if (percentChangeInEvictCount > -minimumExpectedGain) {
            tunerLog += " As could not decrease flushes sufficiently.";
          } else {
            tunerLog += " As number of evictions rose significantly.";
          }
          isReverting = true;
        }
        break;
      default:
        // last step was neutral, revert doesn't not apply here
        break;
      }
      if (!isReverting){
        if (percentChangeInEvictCount < maximumAllowedLoss &&
            percentChangeInFlushes < maximumAllowedLoss) {
          // Everything is fine no tuning required
          newTuneDirection = StepDirection.NEUTRAL;
        } else if (percentChangeInMisses > maximumAllowedLoss &&
            percentChangeInFlushes < maximumAllowedLoss) {
          // more misses , increasing cahce size
          newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          tunerLog +=
              "Increasing block cache size as observed increase in number of cache misses.";
        } else if (percentChangeInFlushes > maximumAllowedLoss &&
            percentChangeInMisses < maximumAllowedLoss) {
          // more flushes , increasing memstore size
          newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          tunerLog += "Increasing memstore size as observed increase in number of flushes.";
        } else {
          // Default. Not enough facts to do tuning.
          newTuneDirection = StepDirection.NEUTRAL;
        }
      }
    }
    switch (newTuneDirection) {
    case INCREASE_BLOCK_CACHE_SIZE:
        newBlockCacheSize = context.getCurBlockCacheSize() + step;
        newMemstoreSize = context.getCurMemStoreSize() - step;
        break;
    case INCREASE_MEMSTORE_SIZE:
        newBlockCacheSize = context.getCurBlockCacheSize() - step;
        newMemstoreSize = context.getCurMemStoreSize() + step;
        break;
    default:
        prevFlushCount = blockedFlushCount + unblockedFlushCount;
        prevEvictCount = evictCount;
        prevCacheMissCount = cacheMissCount;
        prevTuneDirection = StepDirection.NEUTRAL;
        return NO_OP_TUNER_RESULT;
    }
    if (newMemstoreSize > globalMemStorePercentMaxRange) {
      newMemstoreSize = globalMemStorePercentMaxRange;
    } else if (newMemstoreSize < globalMemStorePercentMinRange) {
      newMemstoreSize = globalMemStorePercentMinRange;
    }
    if (newBlockCacheSize > blockCachePercentMaxRange) {
      newBlockCacheSize = blockCachePercentMaxRange;
    } else if (newBlockCacheSize < blockCachePercentMinRange) {
      newBlockCacheSize = blockCachePercentMinRange;
    }
    TUNER_RESULT.setBlockCacheSize(newBlockCacheSize);
    TUNER_RESULT.setMemstoreSize(newMemstoreSize);
    LOG.info(tunerLog);
    prevFlushCount = blockedFlushCount + unblockedFlushCount;
    prevEvictCount = evictCount;
    prevCacheMissCount = cacheMissCount;
    prevTuneDirection = newTuneDirection;
    return TUNER_RESULT;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.step = conf.getFloat(STEP_KEY, DEFAULT_STEP_VALUE);
    this.minimumExpectedGain = conf.getFloat(MIN_EXPECTED_GAIN_KEY,
        DEFAULT_MIN_EXPECTED_GAIN_VALUE);
    this.maximumAllowedLoss = conf.getFloat(MAX_ALLOWED_LOSS_KEY, DEFAULT_MAX_ALLOWED_LOSS_VALUE);
    this.sufficientMemoryLevel = conf.getFloat(SUFFICIENT_MEMORY_LEVEL_KEY,
        DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE);
    this.blockCachePercentMinRange = conf.getFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.blockCachePercentMaxRange = conf.getFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.globalMemStorePercentMinRange = conf.getFloat(MEMSTORE_SIZE_MIN_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
    this.globalMemStorePercentMaxRange = conf.getFloat(MEMSTORE_SIZE_MAX_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
  }

  private enum StepDirection{
    // block cache size was increased
    INCREASE_BLOCK_CACHE_SIZE,
    // memstore size was increased
    INCREASE_MEMSTORE_SIZE,
    // no operation was performed
    NEUTRAL
  }
}
