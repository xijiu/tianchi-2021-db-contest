package com.aliyun.adb.contest;


import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * 硬盘存储块
 */
public class DiskBlock {

  public static volatile String workspaceDir = null;

  private int col;

  private int blockIndex;

  private byte bytePrev;

  private static final int thresholdValue = 4096 * 3;

  public static final short cacheLength = thresholdValue + 2000;

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

  private ByteBuffer batchWriteBuffer = null;

  private long[] temporaryArr = new long[splitNum];

  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) (blockIndex >> (MyAnalyticDB.power - 8 + 1));
    if (MyAnalyticDB.isFirstInvoke) {
      batchWriteBuffer = ByteBuffer.allocateDirect((int) (secondCacheLength * 6.5 + 7));

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

  public synchronized void storeLongArr1(long[] dataArr, int beginIndex, int length) throws Exception {
    int endIndex = beginIndex + length;
    for (int i = beginIndex; i < endIndex; i++) {
      long data = dataArr[i];
      // 2 part  : 36028797018963968L   >> 55
      // 4 part  : 54043195528445952L   >> 54
      // 8 part  : 63050394783186944L   >> 53
      // 16 part : 67553994410557440L   >> 52
      // 32 part : 69805794224242688L   >> 51
      int index = (int) ((data & 67553994410557440L) >> 52);
      dataCache1[index][dataCacheLen1[index]++] = data;
//      if (dataCacheLen1[index] == cacheLength) {
//        putToByteBuffer(index, dataCache1[index], dataCacheLen1[index]);
//        batchWriteBuffer.flip();
//
//        partFileChannel.write(batchWriteBuffer, partFilePosArr[index]);
//        partFilePosArr[index] += batchWriteBuffer.limit();
//        dataCacheLen1[index] = 0;
//      }
    }

    for (int i = 0; i < splitNum; i++) {
      if (dataCacheLen1[i] >= thresholdValue) {
        int lengthTest = dataCacheLen1[i] % 2 == 0 ? dataCacheLen1[i] : dataCacheLen1[i] - 1;
        putToByteBuffer(i, dataCache1[i], lengthTest);
        batchWriteBuffer.flip();

        partFileChannel.write(batchWriteBuffer, partFilePosArr[i]);
        partFilePosArr[i] += batchWriteBuffer.limit();
        if (dataCacheLen1[i] % 2 == 0) {
          dataCacheLen1[i] = 0;
        } else {
          dataCache1[0] = dataCache1[dataCacheLen1[i] - 1];
          dataCacheLen1[i] = 1;
        }
      }
    }
  }

  public synchronized void storeLongArr2(long[] dataArr, int beginIndex, int length) throws Exception {
    int endIndex = beginIndex + length;
    for (int i = beginIndex; i < endIndex; i++) {
      long data = dataArr[i];
      int index = (int) ((data & 67553994410557440L) >> 52);
      dataCache2[index][dataCacheLen2[index]++] = data;
//      if (dataCacheLen2[index] == secondCacheLength) {
//        putToByteBuffer(index, dataCache2[index], dataCacheLen2[index]);
//        batchWriteBuffer.flip();
//
//        partFileChannel.write(batchWriteBuffer, partFilePosArr[index]);
//        partFilePosArr[index] += batchWriteBuffer.limit();
//        dataCacheLen2[index] = 0;
//      }
    }

    for (int i = 0; i < splitNum; i++) {
      if (dataCacheLen2[i] >= thresholdValue) {
        int lengthTest = dataCacheLen2[i] % 2 == 0 ? dataCacheLen2[i] : dataCacheLen2[i] - 1;
        putToByteBuffer(i, dataCache2[i], lengthTest);
        batchWriteBuffer.flip();

        partFileChannel.write(batchWriteBuffer, partFilePosArr[i]);
        partFilePosArr[i] += batchWriteBuffer.limit();
        dataCacheLen2[i] = 0;

        if (dataCacheLen2[i] % 2 == 0) {
          dataCacheLen2[i] = 0;
        } else {
          dataCache2[0] = dataCache2[dataCacheLen2[i] - 1];
          dataCacheLen2[i] = 1;
        }
      }
    }
  }


  public synchronized void forceStoreLongArr1() throws Exception {
    for (int i = 0; i < splitNum; i++) {
      int len = dataCacheLen1[i];
      if (len > 0) {
        putToByteBuffer(i, dataCache1[i], len);
        storeLastData(i);
        batchWriteBuffer.flip();

        partFileChannel.write(batchWriteBuffer, partFilePosArr[i]);
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
        batchWriteBuffer.flip();

        partFileChannel.write(batchWriteBuffer, partFilePosArr[i]);
        partFilePosArr[i] += batchWriteBuffer.limit();
        dataCacheLen2[i] = 0;
      }
    }
  }

  private void storeLastData(int idx) {
    long data = temporaryArr[idx];
    if (data != 0) {
      batchWriteBuffer.put((byte)(data >> 48));
      batchWriteBuffer.put((byte)(data >> 40));
      batchWriteBuffer.put((byte)(data >> 32));
      batchWriteBuffer.putInt((int)(data));
    }
  }

  private void putToByteBuffer(int idx, long[] dataArr, int length) {
    batchWriteBuffer.clear();
    int actualLen = length % 2 == 0 ? length : length - 1;
    for (int i = 0; i < actualLen; i += 2) {
      long data1 = dataArr[i];
      long data2 = dataArr[i + 1];

      batchWriteBuffer.put((byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60)));
      batchWriteBuffer.putInt((int) (data1 << 16 >>> 32));
      batchWriteBuffer.putShort((short) (data1));

      batchWriteBuffer.putInt((int) (data2 << 16 >>> 32));
      batchWriteBuffer.putShort((short) (data2));
    }

    // 奇数
    if (actualLen != length) {
      if (temporaryArr[idx] == 0) {
        temporaryArr[idx] = dataArr[length - 1];
      } else {
        long data1 = temporaryArr[idx];
        long data2 = dataArr[length - 1];

        batchWriteBuffer.put((byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60)));
        batchWriteBuffer.putInt((int) (data1 << 16 >>> 32));
        batchWriteBuffer.putShort((short) (data1));

        batchWriteBuffer.putInt((int) (data2 << 16 >>> 32));
        batchWriteBuffer.putShort((short) (data2));

        temporaryArr[idx] = 0;
      }
    }
  }











  private static ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(perReadSize));

  public long get2(int index) throws Exception {
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

    long[] data = MyAnalyticDB.helper.get();
    ByteBuffer byteBuffer = threadLocal.get();
    byte[] array = byteBuffer.array();
    int idx = 0;
    long pos = partIndex * partFileSize;
    int length = 0;
    int endPos = partFilePosArr[partIndex];
    boolean over = false;
    while (true) {
      byteBuffer.clear();
      int flag = partFileChannel.read(byteBuffer, pos);
      if (flag == -1) {
        break;
      }
      pos += perReadSize;
      length = byteBuffer.position();
      if (pos > endPos) {
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
      }
      if (over) {
        break;
      }
    }

    if (length % 13 != 0) {
      data[idx++] = makeLong2(array[length - 7], array[length - 6], array[length - 5],
              array[length - 4], array[length - 3], array[length - 2], array[length - 1]);
    }

    long solve = tryToQuickFindK(partNum, data, idx, index);
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
