package com.bplus.fudger;

import com.bplus.bptree.BPlusConfiguration;
import com.bplus.bptree.BPlusTree;
import com.bplus.bptree.BPlusTreePerformanceCounter;
import com.bplus.bptree.SearchResult;
import com.bplus.util.InvalidBTreeStateException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

public class Main {

  private static long fileSizeSum = 0;
  private static long buildCostSum = 0;
  private static long readIndexCostSum = 0;
  private static long searchCostSum = 0;
  private static long readDataCostSum = 0;

  private static final List<Long> times = new ArrayList<>();
  private static final List<Long> values = new ArrayList<>();

  private static void readData(String sourceFilePath, int N) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(sourceFilePath))) {
      int cnt = 0;
      String line;
      while ((line = reader.readLine()) != null && cnt < N) {
        String[] split = line.split(",");
        times.add(Long.parseLong(split[0]));
        values.add(Long.parseLong(split[1]));
        cnt++;
      }
    }
  }

  private static void writeData(String targetFilePath, int N) {
    try (DataOutputStream outputStream = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(targetFilePath)))) {
      for (int i = 0; i < N; i++) {
        outputStream.writeLong(times.get(i));
        outputStream.writeLong(values.get(i));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void testOnce(int N, Random random, String sourceFilePath, String targetFilePath,
      String btFilePath) throws IOException, InvalidBTreeStateException {
    readData(sourceFilePath, N);

    writeData(targetFilePath, N);

    long start = System.nanoTime();
    BPlusConfiguration btconf = new BPlusConfiguration();
    BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(false);
    BPlusTree bt = new BPlusTree(btconf, "rw+", btFilePath, bPerf);

    for (int i = 0; i < N; i++) {
      bt.insertKey(times.get(i), Integer.toString(i), true);
    }
    bt.commitTree();
    File file = new File(btFilePath);
    long fileSize = file.length();
    long buildCost = System.nanoTime() - start;
    buildCostSum += buildCost;
    fileSizeSum += fileSize;
    System.out.printf("file size: %d\nB", file.length());
    System.out.printf("Build cost: %dns\n", buildCost);

    start = System.nanoTime();
    bt = new BPlusTree(btconf, "rw", btFilePath, bPerf);
    long readIndexCost = System.nanoTime() - start;
    readIndexCostSum += readIndexCost;
    System.out.printf("Read index cost: %dns\n", readIndexCost);

    start = System.nanoTime();
    long keyToSearch = times.get(random.nextInt(times.size()));
    SearchResult searchResult = bt.searchKey(keyToSearch, true);
    long searchedPos = Long.parseLong(searchResult.getValues().get(0).replaceAll("\\s", ""));
    long searchCost = System.nanoTime() - start;
    searchCostSum += searchCost;
    System.out.printf("Search cost: %dns\n", searchCost);

    start = System.nanoTime();
    try (DataInputStream inputStream = new DataInputStream(
        new BufferedInputStream(new FileInputStream(targetFilePath)))) {
      inputStream.skipBytes((int) (searchedPos * 16));
      long time = inputStream.readLong();
      long value = inputStream.readLong();
      long readCost = System.nanoTime() - start;
      readDataCostSum += readCost;
      System.out.printf("Read cost: %dns\n", readCost);
      System.out.printf("Expected: %d-%d, actual: %d-%d\n", times.get((int) searchedPos),
          values.get((int) searchedPos), time, value);
    }
  }

  private static void testOnce_ChunkIndexStepRegress(int N, Random random, String sourceFilePath,
      String targetFilePath, String btFilePath) throws IOException {
    readData(sourceFilePath, N);

    writeData(targetFilePath, N);

    StepRegress stepRegress = new StepRegress();
    long start = System.nanoTime();
    for (int i = 0; i < N; i++) {
      stepRegress.insert(times.get(i));
    }
    stepRegress.learn();
    // serialize stepRegress: slope,the number of segment keys, a list of segment keys
    try (OutputStream os = Files.newOutputStream(new File("chunkIndexFile").toPath())) {
      // write segmentNum first
      os.write(BytesUtils.longToBytes(
          stepRegress.getSegmentKeys().size())); // not int, int variable length
      os.write(BytesUtils.doubleToBytes(stepRegress.getSlope()));
      for (int i = 0; i < stepRegress.getSegmentKeys().size(); i++) {
        os.write(BytesUtils.doubleToBytes(stepRegress.getSegmentKeys().get(i)));
      }
      os.flush();
    }
    long buildCost = System.nanoTime() - start;
    buildCostSum += buildCost;
    File file = new File("chunkIndexFile");
    long fileSize = file.length();
    fileSizeSum += fileSize;
    System.out.printf("file size: %d\nB", file.length());
    System.out.printf("Build cost: %dns\n", buildCost);

    Path path = Paths.get("chunkIndexFile");
    StepRegress stepRegress2 = new StepRegress();
    DoubleArrayList segmentKeys = new DoubleArrayList();
    start = System.nanoTime();
    // deserialize stepRegress from disk
    byte[] data = Files.readAllBytes(path);
    long segmentKeyNum = BytesUtils.bytesToLong(data, 0, 8 * 8);
    double slope = BytesUtils.bytesToDouble(data, 8);
    stepRegress2.setSlope(slope);
    for (int i = 0; i < segmentKeyNum; i++) {
      segmentKeys.add(BytesUtils.bytesToDouble(data, (i + 2) * 8));
    }
    stepRegress2.setSegmentKeys(segmentKeys);
    if (segmentKeyNum > 1) {
      stepRegress2.inferInterceptsFromSegmentKeys();
    }
    long readIndexCost = System.nanoTime() - start;
    readIndexCostSum += readIndexCost;
    System.out.printf("Read index cost: %dns\n", readIndexCost);

    start = System.nanoTime();
    long keyToSearch = times.get(random.nextInt(times.size()));
    // search stepRegress to get estimated position
    long searchedPos = Math.round(stepRegress2.infer(keyToSearch)); // start from 1
    long searchCost = System.nanoTime() - start;
    searchCostSum += searchCost;
    System.out.printf("Search cost: %dns\n", searchCost);

    start = System.nanoTime();
    try (DataInputStream inputStream = new DataInputStream(
        new BufferedInputStream(Files.newInputStream(Paths.get(targetFilePath))))) {
      // already know that error is small, within 10
      int error = 5;
      if (searchedPos - error > 1) {
        inputStream.skipBytes((int) ((searchedPos - error - 1) * 16));
      }

      long time;
      long value;
      while (true) {
        time = inputStream.readLong();
        value = inputStream.readLong();
        if (time >= keyToSearch) {
          break;
        }
      }
      long readCost = System.nanoTime() - start;
      readDataCostSum += readCost;
      System.out.printf("Read cost: %dns\n", readCost);
      System.out.printf("Expected: %d, actual: %d\n", keyToSearch, time);
      if (keyToSearch != time) {
        throw new IOException("something is wrong with chunk index");
      }
    }
  }

  public static void main(String[] args) throws IOException, InvalidBTreeStateException {
    if (args.length != 3) {
      throw new IOException(
          "Three parameters are needed:\n"
              + "paramer 1: true to test B+ tree, false to test chunk index with step regression\n"
              + "paramer 2: number of keys\n" + "parameter 3: test data file path");
    }
    boolean testBPlusTree = Boolean.parseBoolean(args[0]); // false to test our chunk index
    int N = Integer.parseInt(args[1]); // number of keys
    String sourceFilePath = args[2]; // D:\full-game\BallSpeed.csv

    Random random = new Random();
    String targetFilePath = "test.dat";
    String btFilePath = "test.bp";

    int trialNum = 10;
    for (int i = 0; i < trialNum; i++) {
      if (testBPlusTree) {
        testOnce(N, random, sourceFilePath, targetFilePath, btFilePath);
      } else {
        testOnce_ChunkIndexStepRegress(N, random, sourceFilePath, targetFilePath, btFilePath);
      }
    }
    System.out.println("--------------------------------------------------");
    if (testBPlusTree) {
      System.out.println("[Experimental Settings]");
      System.out.println("B+ tree index");
      System.out.println("on " + N + " keys");
      System.out.println("data source file: " + sourceFilePath);
    } else {
      System.out.println("[Experimental Settings]");
      System.out.println("Chunk index with step regression");
      System.out.println("on " + N + " keys");
      System.out.println("data source file is " + sourceFilePath);
    }
    System.out.println();
    System.out.println("[Experimental Results]");
    System.out.printf(
        "file size: %d, build cost: %d, read index cost: %d, search cost: %d, " +
            "read data cost: %d, " + "total query cost: %d\n",
        fileSizeSum / trialNum, buildCostSum / trialNum,
        readIndexCostSum / trialNum, searchCostSum / trialNum, readDataCostSum / trialNum,
        readIndexCostSum / trialNum + searchCostSum / trialNum + readDataCostSum / trialNum
    );
    System.out.println("--------------------------------------------------");
  }

}
