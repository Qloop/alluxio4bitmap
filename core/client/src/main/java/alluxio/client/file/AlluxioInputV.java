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
package alluxio.client.file;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * Created by Beidou on 2017/2/16.
 */
@NotThreadSafe
public final class AlluxioInputV implements Input {

  private FileInStream is;
  private byte[] b;


  public AlluxioInputV(FileInStream fileInStream) {
    is = fileInStream;
    b = new byte[1];

  }

  @Override
  public final int readByte() throws IOException {

    is.read(b);
    return b[0] & 0xff;
  }

  @Override
  public final boolean readBool() throws IOException {
    return readByte() == 1;
  }

  @Override
  public final int readShort() throws IOException {
//    if (buffer.remaining() >= Short.BYTES) {
//      return buffer.getShort() & 0xffff;
//    } else {
//      buffer.compact();
//      is.read(b, 0, buffer.remaining());
//      buffer.put(b, 0, buffer.remaining());
//      buffer.flip();
//      return buffer.getShort() & 0xffff;
//    }
    return 0;
  }

  @Override
  public final int readInt() throws IOException {
    return is.read();
  }

  @Override
  public float readFloat() throws IOException {
    return 0;
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override
  public double readDouble() throws IOException {
    return 0;
  }

  @Override
  public String readString() throws IOException {
    return null;
  }


  @Override
  public final void close() throws IOException {
    if (is != null) {
      is.close();
      is = null;
    }

  }

  @Override
  public int readByte(int pos) throws IOException {
    return 0;
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    return false;
  }

  @Override
  public int readShort(int pos) throws IOException {
    return 0;
  }

  @Override
  public int readInt(int pos) throws IOException {
    return 0;
  }

  @Override
  public float readFloat(int pos) throws IOException {
    return 0;
  }

  @Override
  public long readLong(int pos) throws IOException {
    return 0;
  }

  @Override
  public double readDouble(int pos) throws IOException {
    return 0;
  }

  @Override
  public String readString(int pos) throws IOException {
    return null;
  }
}
