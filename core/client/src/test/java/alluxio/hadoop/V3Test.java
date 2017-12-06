/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class created on 2017/11/28.
 *
 * @author Connery
 */
public class V3Test {
  private final static long DEFAULT_BUFFER_SIZE = 1_000_000l;

  @Test
  @Ignore
  public void testWrite() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");

    alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();//get fileSystem
    FileOutStream fos = null;
    AlluxioURI uri = new AlluxioURI("/v3test.txt");
    if (fs.exists(uri)) {
      fs.delete(uri);
    }
    CreateFileOptions options = CreateFileOptions.defaults();
    options.setWriteType(WriteType.MUST_CACHE);//set storage to memmory
    fos = fs.createFile(uri, options);
    long start = System.currentTimeMillis();
    String a = "abcd";
    String b = "efg";
    String c = "opqsef";
    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    byteBuffer.putInt(a.getBytes().length);
    byteBuffer.put(a.getBytes());
    byteBuffer.putInt(b.getBytes().length);
    byteBuffer.put(b.getBytes());
    byteBuffer.putInt(c.getBytes().length);
    byteBuffer.put(c.getBytes());
    fos.write(byteBuffer.array(), 0, byteBuffer.position());


    fos.flush();
    fos.close();
    System.out.println("total time:" + (System.currentTimeMillis() - start));
    Configuration.set(PropertyKey.USER_FILE_IN_STREAM_VERSION, 3);
    FileInStream is = fs.openFile(uri, OpenFileOptions.defaults());
    int first = is.readInt();
    System.out.println("" + first);
    is.skip(first);
    int sec = is.readInt();
    is.skip(sec);
    System.out.println("" + sec);
    int th = is.readInt();
    System.out.println("" + th);
    byte[] v = new byte[th];
    is.read(v);
    System.out.println(new String(v));
  }
}
