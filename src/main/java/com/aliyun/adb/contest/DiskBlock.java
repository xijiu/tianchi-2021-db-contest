package com.aliyun.adb.contest;


import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * 硬盘存储块
 */
public class DiskBlock {

  public static volatile String workspaceDir = null;

  private int col;

  private int blockIndex;

  private byte bytePrev;

  public static final short cacheLength = 4096 * 3;

  public static final short secondCacheLength = (int) (cacheLength);

  private final String tableName;

  private static final int perReadSize = 13 * 1024 * 64;

  public static final int partFileSize = 4000000;

//  private static final int concurrentQueryThreadNum = 2;
//
//  private final long[] beginReadPosArr = new long[concurrentQueryThreadNum];
//
//  private final int[] readSizeArr = new int[concurrentQueryThreadNum];
//
//  private static final ThreadPoolExecutor executor = MyAnalyticDB.executor;

  public static final int splitNum = 16;

  public int[] partFilePosArr = new int[splitNum];

  public volatile FileChannel partFileChannel = null;

  private long[][] dataCache1 = null;

  private short[] dataCacheLen1 = null;

  private long[][] dataCache2 = null;

  private short[] dataCacheLen2 = null;

  private byte[] batchWriteArr = null;

  private ByteBuffer batchWriteBuffer = null;

  private long[] temporaryArr = new long[splitNum];

  private static long[] testFirstBucketArr = new long[500000];
  private static int testFirstBucketArrIndex = 0;


  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) (blockIndex >> (MyAnalyticDB.power - 8 + 1));
    if (MyAnalyticDB.isFirstInvoke) {
      batchWriteArr = new byte[(int) (secondCacheLength * 6.5 + 7)];
      batchWriteBuffer = ByteBuffer.wrap(batchWriteArr);

      if (col == 1) {
        dataCache1 = new long[splitNum][cacheLength];
        dataCacheLen1 = new short[splitNum];
      } else {
        dataCache2 = new long[splitNum][secondCacheLength];
        dataCacheLen2 = new short[splitNum];
      }
      for (int i = 0; i < splitNum; i++) {
        partFilePosArr[i] = i * partFileSize;
      }
    }
    this.initFileChannel();
  }

  public synchronized void storeLongArr1(long[] dataArr, int length) throws Exception {
    for (int i = 0; i < length; i++) {
      long data = dataArr[i];
      // 2 part  : 36028797018963968L   >> 55
      // 4 part  : 54043195528445952L   >> 54
      // 8 part  : 63050394783186944L   >> 53
      // 16 part : 67553994410557440L   >> 52
      // 32 part : 69805794224242688L   >> 51
      int index = (int) ((data & 67553994410557440L) >> 52);
      if (blockIndex == 0 && index == 0 && tableName.equals("1")) {
        testFirstBucketArr[testFirstBucketArrIndex++] = data;
      }
      short pos = dataCacheLen1[index]++;
      dataCache1[index][pos] = data;
      if (pos + 1 == cacheLength) {
        putToByteBuffer(index, dataCache1[index], cacheLength);

        partFileChannel.write(batchWriteBuffer, partFilePosArr[index]);
        if (blockIndex == 0 && index == 0 && tableName.equals("1")) {
          System.out.println("load batchWriteArr record begin -----");
          System.out.println(dataCache1[index][0]);
          System.out.println(dataCache1[index][1]);
          for (int j = 0; j < 13; j++) {
            System.out.println(batchWriteArr[j]);
          }
          System.out.println("load batchWriteArr record end -----");
        }
        partFilePosArr[index] += batchWriteBuffer.limit();
        dataCacheLen1[index] = 0;
      }
    }
  }

  public synchronized void storeLongArr2(long[] dataArr, int length) throws Exception {
    for (int i = 0; i < length; i++) {
      long data = dataArr[i];
      int index = (int) ((data & 67553994410557440L) >> 52);
      short pos = dataCacheLen2[index]++;
      dataCache2[index][pos] = data;
      if (pos + 1 == secondCacheLength) {
        putToByteBuffer(index, dataCache2[index], secondCacheLength);
        partFileChannel.write(batchWriteBuffer, partFilePosArr[index]);
        partFilePosArr[index] += batchWriteBuffer.limit();
        dataCacheLen2[index] = 0;
      }
    }
  }


  public synchronized void forceStoreLongArr1() throws Exception {
    for (int i = 0; i < splitNum; i++) {
      int len = dataCacheLen1[i];
      if (len > 0) {
        putToByteBuffer(i, dataCache1[i], len);
        storeLastData(i);
        partFileChannel.write(batchWriteBuffer);
        partFilePosArr[i] += batchWriteBuffer.limit();
        dataCacheLen1[i] = 0;
      }
    }
  }

  public synchronized void forceStoreLongArr2() throws Exception {
    for (int i = 0; i < splitNum; i++) {
      int len = dataCacheLen2[i];
      if (len > 0) {
        putToByteBuffer(i, dataCache2[i], len);
        storeLastData(i);
        partFileChannel.write(batchWriteBuffer);
        partFilePosArr[i] += batchWriteBuffer.limit();
        dataCacheLen2[i] = 0;
      }
    }
  }

  private void storeLastData(int idx) {
    long data = temporaryArr[idx];
    if (data != 0) {
      int index = batchWriteBuffer.limit();
      batchWriteArr[index++] = (byte)(data >> 48);
      batchWriteArr[index++] = (byte)(data >> 40);
      batchWriteArr[index++] = (byte)(data >> 32);
      batchWriteArr[index++] = (byte)(data >> 24);
      batchWriteArr[index++] = (byte)(data >> 16);
      batchWriteArr[index++] = (byte)(data >> 8);
      batchWriteArr[index++] = (byte)(data);

      batchWriteBuffer.clear();
      batchWriteBuffer.position(index);
      batchWriteBuffer.flip();
    }
  }



//  private void putToByteBuffer(long[] data, int length) {
//    int index = 0;
//    for (int i = 0; i < length; i++) {
//      long element = data[i];
//      batchWriteArr[index++] = (byte)(element >> 48);
//      batchWriteArr[index++] = (byte)(element >> 40);
//      batchWriteArr[index++] = (byte)(element >> 32);
//      batchWriteArr[index++] = (byte)(element >> 24);
//      batchWriteArr[index++] = (byte)(element >> 16);
//      batchWriteArr[index++] = (byte)(element >> 8);
//      batchWriteArr[index++] = (byte)(element);
//    }
//
//    batchWriteBuffer.clear();
//    batchWriteBuffer.position(index);
//    batchWriteBuffer.flip();
//  }


  private void putToByteBuffer(int idx, long[] dataArr, int length) {
    int actualLen = length % 2 == 0 ? length : length - 1;
    int index = 0;
    for (int i = 0; i < actualLen; i += 2) {
      long data1 = dataArr[i];
      long data2 = dataArr[i + 1];

      batchWriteArr[index++] = (byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60));
      batchWriteArr[index++] = (byte)(data1 >> 40);
      batchWriteArr[index++] = (byte)(data1 >> 32);
      batchWriteArr[index++] = (byte)(data1 >> 24);
      batchWriteArr[index++] = (byte)(data1 >> 16);
      batchWriteArr[index++] = (byte)(data1 >> 8);
      batchWriteArr[index++] = (byte)(data1);

      batchWriteArr[index++] = (byte)(data2 >> 40);
      batchWriteArr[index++] = (byte)(data2 >> 32);
      batchWriteArr[index++] = (byte)(data2 >> 24);
      batchWriteArr[index++] = (byte)(data2 >> 16);
      batchWriteArr[index++] = (byte)(data2 >> 8);
      batchWriteArr[index++] = (byte)(data2);
    }

    // 奇数
    if (actualLen != length) {
      if (temporaryArr[idx] == 0) {
        temporaryArr[idx] = dataArr[length - 1];
      } else {
        long data1 = temporaryArr[idx];
        long data2 = dataArr[length - 1];

        batchWriteArr[index++] = (byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60));
        batchWriteArr[index++] = (byte)(data1 >> 40);
        batchWriteArr[index++] = (byte)(data1 >> 32);
        batchWriteArr[index++] = (byte)(data1 >> 24);
        batchWriteArr[index++] = (byte)(data1 >> 16);
        batchWriteArr[index++] = (byte)(data1 >> 8);
        batchWriteArr[index++] = (byte)(data1);

        batchWriteArr[index++] = (byte)(data2 >> 40);
        batchWriteArr[index++] = (byte)(data2 >> 32);
        batchWriteArr[index++] = (byte)(data2 >> 24);
        batchWriteArr[index++] = (byte)(data2 >> 16);
        batchWriteArr[index++] = (byte)(data2 >> 8);
        batchWriteArr[index++] = (byte)(data2);

        temporaryArr[idx] = 0;
      }
    }

    batchWriteBuffer.clear();
    batchWriteBuffer.position(index);
    batchWriteBuffer.flip();
  }

//  public long get2(int index, int count) throws Exception {
//    long[] data = MyAnalyticDB.helper.get();
//
//    FileChannel partFileChannel = null;
//    long lastTmpSize = 0;
//    long tmpSize = 0;
//    for (int i = 0; i < splitNum; i++) {
//      tmpSize += partFileChannels[i].size();
//      if (tmpSize > index * 7L) {
//        partFileChannel = partFileChannels[i];
//        index = (int) (index - (lastTmpSize / 7));
//        break;
//      }
//      lastTmpSize = tmpSize;
//    }
//
//    long fileLen = partFileChannel.size();
//
//    fillThreadReadFileInfo(fileLen);
//
//    Future[] futures = new Future[1];
//    for (int i = 0; i < futures.length; i++) {
//      int finalI = i;
//      FileChannel finalPartFileChannel = partFileChannel;
//      futures[i] = executor.submit(() -> {
//        try {
//          ByteBuffer byteBuffer = threadLocal.get();
//          byte[] array = byteBuffer.array();
//          long pos = beginReadPosArr[finalI];
//          int idx = (int) (pos / 7);
//          int endIdx = idx + readSizeArr[finalI];
//          while (true) {
//            byteBuffer.clear();
//            int flag = finalPartFileChannel.read(byteBuffer, pos);
//            pos += perReadSize;
//            if (flag == -1) {
//              break;
//            }
//            int length = byteBuffer.position();
//            for (int j = 0; j < length; j += 7) {
//              data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
//                      array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
//            }
//            if (idx >= endIdx) {
//              break;
//            }
//          }
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
//      });
//    }
//
//    currentThreadRead(partFileChannel);
//
//    for (Future future : futures) {
//      future.get();
//    }
//    return PubTools.solve(data, 0, (int) (fileLen / 7 - 1), index);
//  }
//
//  private void currentThreadRead(FileChannel fileChannel) throws IOException {
//    int finalI = concurrentQueryThreadNum - 1;
//    long[] data = MyAnalyticDB.helper.get();
//    ByteBuffer byteBuffer = ByteBuffer.allocate(perReadSize);
//    byte[] array = byteBuffer.array();
//    long pos = beginReadPosArr[finalI];
//    int idx = (int) (pos / 7);
//    int endIdx = idx + readSizeArr[finalI];
//    while (true) {
//      byteBuffer.clear();
//      int flag = fileChannel.read(byteBuffer, pos);
//      pos += perReadSize;
//      if (flag == -1) {
//        break;
//      }
//      int length = byteBuffer.position();
//      for (int j = 0; j < length; j += 7) {
//        data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
//                array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
//      }
//      if (idx >= endIdx) {
//        break;
//      }
//    }
//  }
//
//  private void fillThreadReadFileInfo(long fileLen) throws Exception {
//    int count = (int) (fileLen / 7 / concurrentQueryThreadNum);
//    long pos = 0;
//    for (int i = 0; i < concurrentQueryThreadNum; i++) {
//      beginReadPosArr[i] = pos;
//      pos += count * 7L;
//      readSizeArr[i] = count;
//    }
//    readSizeArr[concurrentQueryThreadNum - 1] = (int) ((fileLen / 7) - (count * (concurrentQueryThreadNum - 1)));
//  }

  private static ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(perReadSize));

  public long get2(int index) throws Exception {
//    Arrays.sort(testFirstBucketArr, 0, testFirstBucketArrIndex);
//    System.out.println("testFirstBucketArr len is " + testFirstBucketArrIndex);
//    System.out.println("min number 1 is " + testFirstBucketArr[0]);
//    System.out.println("min number 2 is " + testFirstBucketArr[1]);
//    System.out.println("min number 3 is " + testFirstBucketArr[2]);
    System.out.println("correct ---- begin");
    System.out.println(testFirstBucketArr[0] + " : " + Long.toBinaryString(testFirstBucketArr[0]));
    System.out.println(testFirstBucketArr[1] + " : " + Long.toBinaryString(testFirstBucketArr[1]));
    System.out.println("correct ---- end");

    int lastTmpSize = 0;
    int tmpSize = 0;
    byte partNum = 0;
    byte partIndex = 0;
    int fileLen = 0;
    for (byte i = 0; i < splitNum; i++) {
      fileLen = (int) partFilePosArr[i] - (i * partFileSize);
      tmpSize += fileLen % 13 == 0 ? (fileLen / 13 * 2) : (fileLen / 13 * 2 + 1);
      if (tmpSize > index) {
        partNum = i;
        partIndex = i;
        index = index - lastTmpSize;
        break;
      }
      lastTmpSize = tmpSize;
    }
    partNum = (byte) (partNum << 4);
    System.out.println("partIndex is " + partIndex);
    System.out.println("correct answer is " + testFirstBucketArr[index]);

    long[] data = MyAnalyticDB.helper.get();
    ByteBuffer byteBuffer = threadLocal.get();
    byte[] array = byteBuffer.array();
    int idx = 0;
    long pos = partIndex * partFileSize;
    int length = 0;
    int endPos = partFilePosArr[partIndex];
    System.out.println("target file size is a " + ((endPos - pos)));
    System.out.println("target file size is b " + ((endPos - pos) / 1024 / 1024));
    boolean over = false;
    while (true) {
      byteBuffer.clear();
      System.out.println("read pos is " + pos);
      int flag = partFileChannel.read(byteBuffer, pos);
      if (flag == -1) {
        break;
      }
      pos += perReadSize;
      length = byteBuffer.position();
      if (pos > endPos) {
        System.out.println("pos is " + pos + ", endPos is " + endPos + ", length is " + length);
        length = (int) (perReadSize - (pos - endPos));
        over = true;
      }
      int cycleTime = length / 13;
      for (int i = 0; i < cycleTime; i++) {
        int tmpIdx = i * 13;
        byte first = (byte) (((array[tmpIdx] >> 4) & 15) | partNum);
        byte second = (byte) ((array[tmpIdx] & 15) | partNum);
        data[idx++] = makeLong2(first, array[tmpIdx + 1], array[tmpIdx + 2],
                array[tmpIdx + 3], array[tmpIdx + 4], array[tmpIdx + 5], array[tmpIdx + 6]);
        data[idx++] = makeLong2(second, array[tmpIdx + 7], array[tmpIdx + 8],
                array[tmpIdx + 9], array[tmpIdx + 10], array[tmpIdx + 11], array[tmpIdx + 12]);
        System.out.println("read file byte ---- begin");
        for (int j = 0; j < 13; j++) {
          System.out.println(Long.toBinaryString(array[j]));
        }
        System.out.println("read file byte ---- end");
        if (1 == 1) {
          System.exit(1);
        }
      }
      if (over) {
        break;
      }
    }

    if (length % 13 != 0) {
      data[idx++] = makeLong2(array[length - 7], array[length - 6], array[length - 5],
              array[length - 4], array[length - 3], array[length - 2], array[length - 1]);
    }

    System.out.println("read file content ---- begin");
    System.out.println((((long) bytePrev & 0xff) << 56) | data[0]);
    System.out.println((((long) bytePrev & 0xff) << 56) | data[1]);
    System.out.println((((long) bytePrev & 0xff) << 56) | data[2]);
    System.out.println((((long) bytePrev & 0xff) << 56) | data[3]);
    System.out.println("read file content ---- end");

    System.out.println("arr length is " + idx);

    if (1 == 1) {
      Arrays.sort(data, 0, idx);
      System.out.println("data begin !!!!!!");
      for (int i = 0; i < 100; i++) {
        System.out.println(data[i]);
      }
      System.out.println("data end !!!!!!");
      long result = (((long) bytePrev & 0xff) << 56) | data[index];
      System.out.println("target value is a " + result);
      return result;
    }

    long solve = tryToQuickFindK(partNum, data, idx, index);
//    long solve = -1;
    if (solve == -1) {
      solve = PubTools.solve(data, 0, idx - 1, index);
    }



    return (((long) bytePrev & 0xff) << 56) | solve;
//    return PubTools.quickSelect(data, 0, idx - 1, idx - index);
  }

  private long tryToQuickFindK(byte partNum, long[] data, int length, int index) {
    long min = ((long) partNum & 0xff) << 48;
    long max = min | 4503599627370495L;

    long solve = (long) ((index / (double)length) * (max - min)) + min;
    long leftSolve = (long) (solve - solve * 0.0003);
    long rightSolve = (long) (solve + solve * 0.0003);

//    System.out.println("min is " + min + ", max is " + max + ", solve is " + solve + ", index is " + index);

    int left = 0, middle = 0;

    int validIndex = 0;

    for (int i = 0; i < length; i++) {
      long ele = data[i];
      if (ele <= rightSolve) {
        if (ele < leftSolve) {
          left++;
        } else {
          middle++;
          data[i] = data[validIndex];
          data[validIndex++] = ele;
        }
      }
    }

    if (index > left && index < (left + middle)) {
//      System.out.println("hit, left is " + left + ", mid is " + middle + ", right is " + right + ", index is " + index);
//      MyAnalyticDB.findKStat.incrementAndGet();
      index = index - left;
      return PubTools.solve(data, 0, validIndex - 1, index);
    } else {
//      System.out.println("miss, left is " + left + ", mid is " + middle + ", right is " + right + ", index is " + index);
      return -1;
    }
  }

  public static long makeLong2(byte b6, byte b5, byte b4,
                               byte b3, byte b2, byte b1, byte b0){
    return ((((long)b6 & 0xff) << 48) |
            (((long)b5 & 0xff) << 40) |
            (((long)b4 & 0xff) << 32) |
            (((long)b3 & 0xff) << 24) |
            (((long)b2 & 0xff) << 16) |
            (((long)b1 & 0xff) <<  8) |
            (((long)b0 & 0xff)      ));
  }

  public static long makeLong(byte b7, byte b6, byte b5, byte b4,
                              byte b3, byte b2, byte b1, byte b0){
    return ((((long)b7       ) << 56) |
            (((long)b6 & 0xff) << 48) |
            (((long)b5 & 0xff) << 40) |
            (((long)b4 & 0xff) << 32) |
            (((long)b3 & 0xff) << 24) |
            (((long)b2 & 0xff) << 16) |
            (((long)b1 & 0xff) <<  8) |
            (((long)b0 & 0xff)      ));
  }

  private void initFileChannel() {
    try {
      File path = new File(workspaceDir + "/" + tableName);
      if (!path.exists()) {
        path.mkdirs();
      }

      File file = new File(workspaceDir + "/" + tableName + "/partFile_" + col + "_" + blockIndex + ".data");
      if (!file.exists()) {
        file.createNewFile();
      }
      partFileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }



}
