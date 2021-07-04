package com.aliyun.adb.contest;


import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * 硬盘存储块
 */
public class DiskBlock {

  public static volatile String workspaceDir = null;

  private int col;

  private int blockIndex;

  private byte bytePrev;

  private File file = null;

  private File sortedFile = null;

  private volatile FileChannel fileChannel = null;

  private volatile FileChannel sortedFileChannel = null;

  public static final int cacheLength = 4096 * 3;

  public static final int secondCacheLength = (int) (cacheLength);

  private final String tableName;

//  private final int cacheArrLen = 7 * 1024 * 1024 / 8;
//
//  private final long[] cacheArr = new long[cacheArrLen];

  private volatile int cacheArrIndex = 0;





  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) (blockIndex >> (MyAnalyticDB.power - 8 + 1));
    this.initFileChannel();
  }

  public void storeArr(ByteBuffer byteBuffer) throws Exception {
    fileChannel.write(byteBuffer);
  }

//  public synchronized void storeLongArr(long[] data, int len) throws Exception {
//    if (cacheArrIndex + len >= cacheArrLen) {
//      storeLongArrToDisk();
//      storeLongArr(data, len);
//    } else {
//      System.arraycopy(data, 0, cacheArr, cacheArrIndex, len);
//      cacheArrIndex += len;
//    }
//  }

  private AtomicInteger parSortNum = new AtomicInteger();

//  private void storeLongArrToDisk() throws Exception {
//    Arrays.sort(cacheArr, 0, cacheArrIndex);
//    int num = parSortNum.incrementAndGet();
//    File file = new File(workspaceDir + "/" + tableName + "/partSorted_" + col + "_" + blockIndex + "_" + num + ".data");
//    file.createNewFile();
//    FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE);
//    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1 * 1024 * 1024);
//    for (int i = 0; i < cacheArrIndex; i++) {
//      byteBuffer.putLong(cacheArr[i]);
//      if (!byteBuffer.hasRemaining()) {
//        byteBuffer.flip();
//        fileChannel.write(byteBuffer);
//        byteBuffer.clear();
//      }
//    }
//    byteBuffer.flip();
//    fileChannel.write(byteBuffer);
//    cacheArrIndex = 0;
//  }

  public long get(int index, int count) throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.allocate(7);
    sortedFileChannel.read(byteBuffer, index * 7L);
    byte[] array = byteBuffer.array();
    return makeLong(bytePrev, array[0], array[1], array[2], array[3], array[4], array[5], array[6]);
  }

  public long get2(int index, int count) throws Exception {
    long[] data = helper.get();
    int readSize = 7 * 1024 * 128;
    ByteBuffer byteBuffer = ByteBuffer.allocate(readSize);
    byte[] array = byteBuffer.array();
    int idx = 0;
    long pos = 0;
    while (true) {
      byteBuffer.clear();
      int flag = fileChannel.read(byteBuffer, pos);
      pos += readSize;
      if (flag == -1) {
        break;
      }
      int length = byteBuffer.position();
      for (int i = 0; i < length; i += 7) {
        data[idx++] = makeLong(bytePrev, array[i], array[i + 1], array[i + 2],
                array[i + 3], array[i + 4], array[i + 5], array[i + 6]);
      }
    }

    return PubTools.solve(data, 0, idx - 1, index);
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
      if (fileChannel == null) {
        synchronized (this) {
          if (fileChannel == null) {
            File path = new File(workspaceDir + "/" + tableName);
            if (!path.exists()) {
              path.mkdirs();
            }

            file = new File(workspaceDir + "/" + tableName + "/" + col + "_" + blockIndex + ".data");
            if (!file.exists()) {
              file.createNewFile();
            }
            fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);

            // sorted file
            sortedFile = new File(workspaceDir + "/" + tableName + "/sorted_" + col + "_" + blockIndex + ".data");
            if (!sortedFile.exists()) {
              sortedFile.createNewFile();
            }
            sortedFileChannel = FileChannel.open(sortedFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  private static ThreadLocal<long[]> helper = ThreadLocal.withInitial(() -> new long[8000000]);

//  public void query() throws Exception {
//    int size = (int) (file.length() / 7);
//    long[] result = helper.get();
//    int readLen = 7 * 1024 * 128;
//    ByteBuffer buffer = ByteBuffer.allocate(readLen);
//    byte[] batchWriteArr = buffer.array();
//    int idx = 0;
//    fileChannel.position(0);
//    while (true) {
//      buffer.clear();
//      int flag = fileChannel.read(buffer);
//      if (flag == -1 || flag == 0) {
//        break;
//      }
//      int length = buffer.position();
////      buffer.flip();
//      for (int i = 0; i < length; i += 7) {
//        result[idx++] = DiskBlock.makeLong(
//                bytePrev, batchWriteArr[i], batchWriteArr[i + 1], batchWriteArr[i + 2],
//                batchWriteArr[i + 3], batchWriteArr[i + 4], batchWriteArr[i + 5], batchWriteArr[i + 6]);
//      }
//    }
//
//    if (idx != size) {
//      System.out.println("idx not equals size, idx is " + idx + ", size is " + size);
//    }
//
//    // 1s ~ 2s
////    Arrays.sort(result, 0, size);
//
//    buffer.clear();
//    for (int i = 0; i < size; i++) {
//      if (!buffer.hasRemaining()) {
//        buffer.flip();
//        sortedFileChannel.write(buffer);
//        buffer.clear();
//      }
//      long element = result[i];
//      buffer.put((byte)(element >> 48));
//      buffer.put((byte)(element >> 40));
//      buffer.put((byte)(element >> 32));
//      buffer.put((byte)(element >> 24));
//      buffer.put((byte)(element >> 16));
//      buffer.put((byte)(element >> 8));
//      buffer.put((byte)(element));
//    }
//
//    buffer.flip();
//    sortedFileChannel.write(buffer);
//  }
}
