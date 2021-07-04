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

  private File file = null;

  private volatile FileChannel fileChannel = null;

  public static final int cacheLength = 4096 * 2;

  public static final int secondCacheLength = (int) (cacheLength);

  private final String tableName;




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
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  /**
   * 128- 8000000
   * 256- 4000000
   * 512- 2000000
   * 1024-1000000
   *  */
  private static ThreadLocal<long[]> helper = ThreadLocal.withInitial(() -> new long[2000000]);

}
