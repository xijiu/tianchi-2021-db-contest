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

  public static final int concurrenceQueryDiskNum = 4;

  public static AtomicIntegerArray queryDiskFlag = new AtomicIntegerArray(concurrenceQueryDiskNum);

  public static int[] countArr = new int[concurrenceQueryDiskNum];

  public static int[] indexArr = new int[concurrenceQueryDiskNum];

//  public static volatile FileChannel totalFileChannel = null;
//
//  public static AtomicInteger finishQueryThreadNum = new AtomicInteger();

  private final String tableName;





  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) blockIndex;
  }

  public void storeArr(ByteBuffer byteBuffer) throws Exception {
    initFileChannel();
    fileChannel.write(byteBuffer);
  }

  public long get(int index, int count) throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.allocate(7);
    sortedFileChannel.read(byteBuffer, index * 7L);
    byte[] array = byteBuffer.array();
    return makeLong(bytePrev, array[0], array[1], array[2], array[3], array[4], array[5], array[6]);
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

  private void initFileChannel() throws IOException {
    if (fileChannel == null) {
      synchronized (this) {
        if (fileChannel == null) {
          File path = new File(workspaceDir + "/" + tableName);
          if (!path.exists()) {
            path.mkdirs();
          }

          file = new File(workspaceDir + "/" + tableName + "/" + col + "_" + blockIndex + ".data");
          if (file.exists()) {
            file.delete();
          }
          file.createNewFile();
          fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);

          // sorted file
          sortedFile = new File(workspaceDir + "/" + tableName + "/sorted_" + col + "_" + blockIndex + ".data");
          if (sortedFile.exists()) {
            sortedFile.delete();
          }
          sortedFile.createNewFile();
          sortedFileChannel = FileChannel.open(sortedFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
        }
      }
    }
  }

  private static ThreadLocal<long[]> helper = ThreadLocal.withInitial(() -> new long[8000000]);

  public void query() throws Exception {
    int size = (int) (file.length() / 7);
    long[] result = helper.get();
    int readLen = 7 * 1024 * 128;
    ByteBuffer buffer = ByteBuffer.allocate(readLen);
    byte[] batchWriteArr = buffer.array();
    int idx = 0;
    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
    while (true) {
      buffer.clear();
      int flag = fileChannel.read(buffer);
      if (flag == -1 || flag == 0) {
        System.out.println("flag is " + flag);
        break;
      }
      buffer.flip();
      int length = buffer.position();
      for (int i = 0; i < length; i += 7) {
        result[idx++] = DiskBlock.makeLong(
                bytePrev, batchWriteArr[i], batchWriteArr[i + 1], batchWriteArr[i + 2],
                batchWriteArr[i + 3], batchWriteArr[i + 4], batchWriteArr[i + 5], batchWriteArr[i + 6]);
      }
    }

    System.out.println("sort idx is " + idx);
    System.out.println("sort size is " + size);

    Arrays.sort(result, 0, size);


    buffer.clear();
    for (int i = 0; i < size; i++) {
      if (!buffer.hasRemaining()) {
        buffer.flip();
        sortedFileChannel.write(buffer);
        buffer.clear();
      }
      long element = result[i];
      buffer.put((byte)(element >> 48));
      buffer.put((byte)(element >> 40));
      buffer.put((byte)(element >> 32));
      buffer.put((byte)(element >> 24));
      buffer.put((byte)(element >> 16));
      buffer.put((byte)(element >> 8));
      buffer.put((byte)(element));
    }
  }
}
