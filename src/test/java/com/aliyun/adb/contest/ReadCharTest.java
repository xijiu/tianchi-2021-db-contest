package com.aliyun.adb.contest;

import com.aliyun.adb.contest.utils.PubTools;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class ReadCharTest {

  @Test
  public void test3() throws Exception {
    // lineitem L_ORDERKEY,L_PARTKEY
    // orders   O_ORDERKEY,O_CUSTKEY

    System.out.println(4096 * 3 * 7);
    System.out.println(4096 * 3 * 6.5);
    System.out.println((byte)(15 << 4));
    System.out.println((byte)(15 << 4));


    int partNum = 15;
    partNum = (byte) (partNum << 4);
    byte first = (byte) ((32 >> 4) | partNum);
    System.out.println(Integer.toBinaryString(first));
  }

  @Test
  public void abc() {
    String str = new String("L_ORDERKEY,L_PARTKEY\n");
    System.out.println(str);

    byte[] bytes = str.getBytes();
    System.out.println(Arrays.toString(bytes));
    System.out.println(bytes.length);

    ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    byteBuffer.putLong(8L);
    byteBuffer.flip();
    System.out.println(byteBuffer.array().length);
    System.out.println(byteBuffer.limit());

    long[] arr = new long[2350000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new Random().nextLong();
    }
    long begin = System.currentTimeMillis();
    Arrays.sort(arr);
    System.out.println(System.currentTimeMillis() - begin);
  }


  @Test
  public void abcd() throws Exception {
    // 34100020825  36697399353  48288991621
    // 5533994811806328209(0.6)
    // 3688924663427225681
    // 7378642222410790205(0.8)
    // 8762030145877817422(0.95)
    // 5072796932216911011(0.55)
    // 92263566853817611(0.01)
    // 922067015987522200(0.1)
    // 1383096180205860432(0.15)
    // 1272464117860178902(0.138)


    // 2305591570981736909(0.25)
    // 3689450223303038691(0.4)
    // 7194124025716264465(0.78)
    // 1014401445768370734(0.11)
    File sortedCharFile = new File("/Users/likangning/test/sortedCharFile.data");
    FileChannel fileChannel = FileChannel.open(sortedCharFile.toPath(), StandardOpenOption.READ);
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    fileChannel.position(60000000 * 8 - 8);
    fileChannel.read(byteBuffer);
    byteBuffer.flip();
    System.out.println(byteBuffer.getLong());
    fileChannel.close();
  }

  @Test
  public void aaa() throws IOException {
    System.out.println((long) Math.ceil(300000000L * 0.2D));
  }

  public static void main(String[] args) throws Exception {
    MyAnalyticDB myAnalyticDB = new MyAnalyticDB();
    myAnalyticDB.load("/Users/likangning/test/sourceFile", "/Users/likangning/test/tmp");
    // L_ORDERKEY  L_PARTKEY

    Assert.assertEquals("1272464117860178902", myAnalyticDB.quantile("a", "L_ORDERKEY", 0.138D));
    Assert.assertEquals("1844078352963573465", myAnalyticDB.quantile("a", "L_ORDERKEY", 0.2D));
    Assert.assertEquals("5533994811806328209", myAnalyticDB.quantile("a", "L_ORDERKEY", 0.6D));
    Assert.assertEquals("8762030145877817422", myAnalyticDB.quantile("a", "L_ORDERKEY", 0.95D));

    Assert.assertEquals("2305591570981736909", myAnalyticDB.quantile("a", "L_PARTKEY", 0.25D));
    Assert.assertEquals("7194124025716264465", myAnalyticDB.quantile("a", "L_PARTKEY", 0.78D));
    Assert.assertEquals("1014401445768370734", myAnalyticDB.quantile("a", "L_PARTKEY", 0.11D));
    System.exit(1);
  }

  @Test
  public void aaab() throws IOException {
    long begin = System.currentTimeMillis();
    int len = 200000000;
    long[] arr = new long[len];
    long begin2 = System.currentTimeMillis();

    long endIndex = 3000000000000000L + len;
    int idx = 0;
    for (long i = 3000000000000000L; i < endIndex; i++) {
      arr[idx++] = i;
    }
    long begin3 = System.currentTimeMillis();

    System.out.println("开辟空间耗时： " + (begin2 - begin));
    System.out.println("cpu赋值耗时： " + (begin3 - begin2));
    System.out.println("总耗时： " + (begin3 - begin));
  }

  @Test
  public void aaab2() throws IOException {
    long begin = System.currentTimeMillis();
    int len = 200000000;
    int[] arr1 = new int[len];
    short[] arr2 = new short[len];
    byte[] arr3 = new byte[len];
    long begin2 = System.currentTimeMillis();

    int idx = 0;
    long endIndex = 3000000000000000L + len;
    for (long i = 3000000000000000L; i < endIndex; i++) {
      arr1[idx] = (int) i;
      arr2[idx] = (short) (i >> 32);
      arr3[idx] = (byte) (i >> 48);
      idx++;
    }
    long begin3 = System.currentTimeMillis();

    System.out.println("开辟空间耗时： " + (begin2 - begin));
    System.out.println("cpu赋值耗时： " + (begin3 - begin2));
    System.out.println("总耗时： " + (begin3 - begin));
  }


  @Test
  public void aaab3() throws IOException {
    long[] arr = new long[]{0,18,38,59,79,100,120,141,161,182,202,223,243,264,284,285,305,325,346,366,387,407,428,448,449,469,489,510,530,531,551,571,572,592,612,613,633,653,674,694,715,735,756,776,797,817,818,838,858,859,879,899,920,940,941,961,981,982,1002,1022,1023,1043,1063,1084,1104,1125,1145,1146,1166,1186,1187,1207,1227,1248,1268,1289,1309,1310,1330,1350,1371,1391,1412,1432,1453,1473,1474,1494,1514,1515,1535,1555,1556,1576,1596,1597,1617,1637,1638,1658,1678,1679,1699,1719,1740,1760,1761,1781,1801,1802,1822,1842,1863,1883,1884,1904,1924,1925,1945,1965,1966,1986,2006,2007,2027,2047};
    int k = arr.length - 2;
    System.out.println(PubTools.quickSelect(arr, 0, arr.length - 1, k));;
  }


  @Test
  public void aaab4() throws Exception {
    System.out.println(Integer.parseInt("11100000", 2));
    int standard = Integer.parseInt("11100000", 2);
    System.out.println(standard);
    System.out.println((224 & 224) >> 5);

    for (int i = 0; i < 64; i++) {
      System.out.print("0");
    }
    System.out.println();

    System.out.println("len is " +  "0010101011010000000000000000000001101000101100100000011110000011".length());


    System.out.println(Long.parseLong("0000000011110000000000000000000000000000000000000000000000000000", 2));
    System.out.println(Long.parseLong("0000000000001111000000000000000000000000000000000000000000000000", 2));
    System.out.println(Long.parseLong("0000000011111101000000000000000000000000000000000000000000000000", 2));
    System.out.println(Long.parseLong("0000000011111110000000000000000000000000000000000000000000000000", 2));
    // 36028797018963970
    System.out.println(Long.parseLong("0000000010000000000000000000000000000000000000000000000000000010", 2));


    byte num = 64;

    long l = (((long) num & 0xff) << 56) | 36028797018963970L;

    System.out.println(Long.toBinaryString(l));

  }


}
