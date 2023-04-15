package com.bplus.fudger;

import java.util.NoSuchElementException;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

/**
 * Util for computing median, MAD, percentile
 */
public class TimeExactOrderStatistics {

  private LongArrayList longArrayList;

  public TimeExactOrderStatistics() {
    longArrayList = new LongArrayList();
  }

  public void insert(long timestamp) {
    longArrayList.add(timestamp);
  }

  public double getMedian() {
    return getMedian(longArrayList);
  }

  public double getMad() {
    return getMad(longArrayList);
  }

  public String getPercentile(double phi) {
    return Long.toString(getPercentile(longArrayList, phi));
  }

  public static double getMedian(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return ((nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0);
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static float getPercentile(FloatArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static double getPercentile(DoubleArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static int getPercentile(IntArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = DoubleArrayList.newWithNValues(nums.size(), 0);
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static long getPercentile(LongArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }
}
