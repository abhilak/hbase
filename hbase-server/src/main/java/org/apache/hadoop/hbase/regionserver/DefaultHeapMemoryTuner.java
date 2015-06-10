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
  public static final float DEFAULT_STEP_VALUE = 0.02f; // 2%
  // Fraction change less than this will not be considered harmful
  // TODO dynamically calculate tolerance from standard deviation of percent changes
  public static final float tolerance = 0.00f; // 0%
  // If current block cache size or memstore size in use is below this level relative to memory 
  // provided to it then corresponding component will be considered to have sufficient memory
  public static final float sufficientMemoryLevel = 0.5f; // 50%
  // Large constant value assigned to percent changes when its undefined
  public static final float undefinedPercentChange = 100f;
  private static final TunerResult TUNER_RESULT = new TunerResult(true);
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  private Configuration conf;
  private float step = DEFAULT_STEP_VALUE;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  private StepDirection stepDirection = StepDirection.NEUTRAL;
  private long prevFlushCount = 0;
  private long prevEvictCount = 0;
  private long prevCacheMissCount = 0;
  
  @Override
  public TunerResult tune(TunerContext context) {
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    long cacheMissCount = context.getCacheMissCount();
    // We can consider memstore or block cache to be sufficient if
    // we are using only a minor fraction of what have been already provided to it
    boolean memstoreSufficient = (blockedFlushCount == 0 && unblockedFlushCount == 0)
            || context.getCurMemStoreUsed() < context.getCurMemStoreSize()*sufficientMemoryLevel;
    boolean blockCacheSufficient = evictCount == 0 ||
            context.getCurBlockCacheUsed() < context.getCurBlockCacheSize()*sufficientMemoryLevel;
    if (memstoreSufficient && blockCacheSufficient) {
      prevFlushCount = blockedFlushCount + unblockedFlushCount;
      prevEvictCount = evictCount;
      prevCacheMissCount = cacheMissCount;
      stepDirection = StepDirection.NEUTRAL;
      return NO_OP_TUNER_RESULT;
    }
    float newMemstoreSize;
    float newBlockCacheSize;
    if (memstoreSufficient) {
      // Increase the block cache size and corresponding decrease in memstore size
      stepDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
    } else if (blockCacheSufficient) {
      // Increase the memstore size and corresponding decrease in block cache size
      stepDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
    } else {
      float percentChangeInEvictCount;
      float percentChangeInFlushes;
      float percentChangeInMisses;
      if (prevEvictCount != 0) {
        percentChangeInEvictCount  = (float)(evictCount-prevEvictCount)/(float)(prevEvictCount);
      } else {
        // current and prev are both cannot be zero, assinging large percent change
        percentChangeInEvictCount  = undefinedPercentChange;
      }
      if (prevFlushCount != 0) {
        percentChangeInFlushes = (float)(blockedFlushCount + unblockedFlushCount - 
            prevFlushCount)/(float)(prevFlushCount);
      } else {
        percentChangeInFlushes = undefinedPercentChange;
      }
      if (prevCacheMissCount != 0) {
        percentChangeInMisses = (float)(cacheMissCount-prevCacheMissCount)/(float)(prevCacheMissCount);
      } else {
        percentChangeInMisses = undefinedPercentChange;
      }
      boolean isReverting = false;
      switch (stepDirection) {
      case INCREASE_BLOCK_CACHE_SIZE:
        if (percentChangeInEvictCount + percentChangeInFlushes > tolerance) {
          // reverting previous step as it was not useful
          stepDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          isReverting = true;
        }
        break;
      case INCREASE_MEMSTORE_SIZE:
        if (percentChangeInEvictCount + percentChangeInFlushes > tolerance) {
          // reverting previous step as it was not useful
          stepDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          isReverting = true;
        }
        break;
      default:
        // last step was neutral, revert doesn't not apply here
        break;
      }
      if (!isReverting){
        if (percentChangeInEvictCount < 0 && percentChangeInFlushes < 0) {
          // Everything is fine no tuning required
          stepDirection = StepDirection.NEUTRAL;
        } else if (percentChangeInMisses > percentChangeInFlushes + tolerance) {
          // more misses , increasing cahce size
          stepDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
        } else if (percentChangeInFlushes > percentChangeInMisses + tolerance){
          // more flushes , increasing memstore size
          stepDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
        } else {
          // Default. Not enough facts to do tuning.
          stepDirection = StepDirection.NEUTRAL;
        }
      }
    }
    switch (stepDirection) {
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
        stepDirection = StepDirection.NEUTRAL;
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
    prevFlushCount = blockedFlushCount + unblockedFlushCount;
    prevEvictCount = evictCount;
    prevCacheMissCount = cacheMissCount;
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
