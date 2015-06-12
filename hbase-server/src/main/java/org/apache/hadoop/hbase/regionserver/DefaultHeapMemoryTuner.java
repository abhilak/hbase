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
  public static final String LOOKUP_PERIODS_KEY =
      "hbase.regionserver.heapmemory.autotuner.lookup.periods";
  public static final float DEFAULT_STEP_VALUE = 0.02f; // 2%
  // Fraction change less than this will not be considered harmful
  public static final float DEFAULT_MAX_ALLOWED_LOSS_VALUE = 0.01f; // 1%
  // Fraction change less than this will not be considered useful
  public static final float DEFAULT_MIN_EXPECTED_GAIN_VALUE = 0.005f; // 0.5%
  // If current block cache size or memstore size in use is below this level relative to memory
  // provided to it then corresponding component will be considered to have sufficient memory
  public static final float DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE = 0.5f; // 50%
  // Large constant value assigned to percent changes when its undefined
  public static final float undefinedPercentChange = 1.0f; // 100%
  // This will cover variations over past 1 hr according to default tuner period
  public static final int DEFAULT_LOOKUP_PERIODS = 60;
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  private Log LOG = LogFactory.getLog(DefaultHeapMemoryTuner.class);
  private TunerResult TUNER_RESULT = new TunerResult(true);
  private Configuration conf;
  private float step = DEFAULT_STEP_VALUE;
  private float maximumAllowedLoss = DEFAULT_MAX_ALLOWED_LOSS_VALUE;
  private float minimumExpectedGain = DEFAULT_MIN_EXPECTED_GAIN_VALUE;
  private float sufficientMemoryLevel = DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE;
  private int tunerLookupPeriods = DEFAULT_LOOKUP_PERIODS;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;
  private RollingStatCalculator rollingStatsForCacheMisses;
  private RollingStatCalculator rollingStatsForFlushes;

  private StepDirection prevTuneDirection = StepDirection.NEUTRAL;
  private long prevFlushCount = 0;
  private long prevEvictCount = 0;

  @Override
  public TunerResult tune(TunerContext context) {
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    long cacheMissCount = context.getCacheMissCount();
    long totalFulshCount = blockedFlushCount+unblockedFlushCount;
    rollingStatsForCacheMisses.insertDataValue(cacheMissCount);
    rollingStatsForFlushes.insertDataValue(totalFulshCount);
    StepDirection newTuneDirection = StepDirection.NEUTRAL;
    String tunerLog = "";
    // We can consider memstore or block cache to be sufficient if
    // we are using only a minor fraction of what have been already provided to it
    boolean earlyMemstoreSufficientCheck = totalFulshCount == 0
            || context.getCurMemStoreUsed() < context.getCurMemStoreSize()*sufficientMemoryLevel;
    boolean earlyBlockCacheSufficientCheck = evictCount == 0 ||
            context.getCurBlockCacheUsed() < context.getCurBlockCacheSize()*sufficientMemoryLevel;
    if (earlyMemstoreSufficientCheck && earlyBlockCacheSufficientCheck) {
      prevFlushCount = totalFulshCount;
      prevEvictCount = evictCount;
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
      if (prevEvictCount != 0) {
        percentChangeInEvictCount  = (float)(evictCount-prevEvictCount)/(float)(prevEvictCount);
      } else {
        // current and previous are both cannot be zero, assigning large percent change
        percentChangeInEvictCount  = undefinedPercentChange;
      }
      if (prevFlushCount != 0) {
        percentChangeInFlushes = (float)(totalFulshCount -
            prevFlushCount)/(float)(prevFlushCount);
      } else {
        percentChangeInFlushes = undefinedPercentChange;
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
        // mean +- deviation/2 is considered to be normal 
        // below it its consider low and above it its considered high
        if ((double)cacheMissCount < rollingStatsForCacheMisses.getMean() -
            rollingStatsForCacheMisses.getDeviation()/2.00 &&
            (double)totalFulshCount < rollingStatsForFlushes.getMean() -
            rollingStatsForFlushes.getDeviation()/2.00) {
          // Everything is fine no tuning required
          newTuneDirection = StepDirection.NEUTRAL;
        } else if ((double)cacheMissCount > rollingStatsForCacheMisses.getMean() +
            rollingStatsForCacheMisses.getDeviation()/2.00 &&
            (double)totalFulshCount < rollingStatsForFlushes.getMean() -
            rollingStatsForFlushes.getDeviation()/2.00) {
          // more misses , increasing cache size
          newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          tunerLog +=
              "Increasing block cache size as observed increase in number of cache misses.";
        } else if ((double)cacheMissCount < rollingStatsForCacheMisses.getMean() -
            rollingStatsForCacheMisses.getDeviation()/2.00 &&
            (double)totalFulshCount > rollingStatsForFlushes.getMean() +
            rollingStatsForFlushes.getDeviation()/2.00) {
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
        prevFlushCount = totalFulshCount;
        prevEvictCount = evictCount;
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
    prevFlushCount = totalFulshCount;
    prevEvictCount = evictCount;
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
    this.tunerLookupPeriods = conf.getInt(LOOKUP_PERIODS_KEY, DEFAULT_LOOKUP_PERIODS);
    this.blockCachePercentMinRange = conf.getFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.blockCachePercentMaxRange = conf.getFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.globalMemStorePercentMinRange = conf.getFloat(MEMSTORE_SIZE_MIN_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
    this.globalMemStorePercentMaxRange = conf.getFloat(MEMSTORE_SIZE_MAX_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
    this.rollingStatsForCacheMisses = new RollingStatCalculator(this.tunerLookupPeriods);
    this.rollingStatsForFlushes = new RollingStatCalculator(this.tunerLookupPeriods);
  }

  private enum StepDirection{
    // block cache size was increased
    INCREASE_BLOCK_CACHE_SIZE,
    // memstore size was increased
    INCREASE_MEMSTORE_SIZE,
    // no operation was performed
    NEUTRAL
  }

  /**
   * This class will help maintaining mean and standard deviation.
   * Initialized with number of rolling periods which basically means number of past periods 
   * whose data will be considered for maintaining mean and deviation. It will take O(N) memory 
   * to maintain it, where N is number of look up periods. It can be used when we have varying 
   * trends of reads and write.
   * If zero is passed then it will maintain mean and variance from the starting point. It will use 
   * O(1) memory only. But note that since it will maintain mean / deviation from the start 
   * they may behave like constants and will ignore short trends.
   */
  public class RollingStatCalculator {
    private double currentSum;
    private double currentSqrSum;
    private long numberOfDataValues;
    private int rollingPeriod;
    private int currentIndexPosition;
    private long [] dataValues;

    public RollingStatCalculator(int rollingPeriod) {
      this.rollingPeriod = rollingPeriod;
      this.dataValues = initializeZeros(rollingPeriod);
      this.currentSum = 0.0;
      this.currentSqrSum = 0.0;
      this.currentIndexPosition = 0;
      this.numberOfDataValues = 0;
    }

    public void insertDataValue(long data) {
      // if current number of data points already equals rolling period
      // remove one data and adjust the every thing
      if(numberOfDataValues >= rollingPeriod && rollingPeriod > 0) {
        this.removeData(dataValues[currentIndexPosition]);
      }
      numberOfDataValues++;
      currentSum = currentSum + (double)data;
      currentSqrSum = currentSqrSum + ((double)data * data);
      if (rollingPeriod >0)
      {
        dataValues[currentIndexPosition] = data;
        currentIndexPosition = (currentIndexPosition + 1) % rollingPeriod;   
      }
    }

    private void removeData(long data) {
      currentSum = currentSum + (double)data;
      currentSqrSum = currentSqrSum + ((double)data * data);
      numberOfDataValues--;
    }

    public double getMean() {
      return this.currentSum / (double)numberOfDataValues;
    }

    public double getDeviation() {
      double variance = (currentSqrSum - (currentSum*currentSum)/(double)(numberOfDataValues))/
          numberOfDataValues;
      return Math.sqrt(variance);
    }

    private long [] initializeZeros(int n) {
      long [] zeros = new long [n];
      for (int i=0; i<n; i++) {
        zeros[i] = 0L;
      }
      return zeros;
    }
  }
}
