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
package alluxio.client.block;

import alluxio.client.file.Input;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * This class created on 2017/12/1.
 *
 * @author Connery
 */
public class BlockInputWrapper implements Input {
  private final static int DEFAULT_BUFFER_SIZE = 81920;

  private InputStream is;
  private ByteBuffer buffer;
  private byte[] b;
  private long mPos;

  public BlockInputWrapper(InputStream is) {
    this.is = is;
    mPos = 0;
    init(DEFAULT_BUFFER_SIZE);
  }

  public static String convert2String(byte[] b) {
    if (b.length == 0) {
      return null;
    }
    try {
      return new String(b, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private void init(int bufferSize) {
    try {
      buffer = ByteBuffer.allocateDirect(bufferSize);
      b = new byte[DEFAULT_BUFFER_SIZE];
      is.read(b);
      buffer.put(b);
      buffer.flip();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readByte() throws IOException {
    if (buffer.remaining() >= Byte.BYTES) {
      int r = buffer.get() & 0xff;
      mPos += Byte.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      int r = buffer.get() & 0xff;
      mPos += Byte.BYTES;
      return r;
    }
  }

  private byte readOrignalByte() throws IOException {
    if (buffer.remaining() >= Byte.BYTES) {
      byte r = buffer.get();
      mPos += Byte.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      byte r = buffer.get();
      mPos += Byte.BYTES;
      return r;
    }
  }

  @Override
  public boolean readBool() throws IOException {
    return readByte() == 1;
  }

  @Override
  public int readShort() throws IOException {
    if (buffer.remaining() >= Short.BYTES) {
      int r = buffer.getShort() & 0xffff;
      mPos += Short.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      int r = buffer.getShort() & 0xffff;
      mPos += Short.BYTES;
      return r;
    }
  }

  @Override
  public int readInt() throws IOException {
    if (buffer.remaining() >= Integer.BYTES) {
      int r = buffer.getInt();
      mPos += Integer.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      int r = buffer.getInt();
      mPos += Integer.BYTES;
      return r;
    }
  }

  @Override
  public float readFloat() throws IOException {
    if (buffer.remaining() >= Float.BYTES) {
      float r = buffer.getFloat();
      mPos += Float.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      float r = buffer.getFloat();
      mPos += Float.BYTES;
      return r;
    }
  }

  @Override
  public long readLong() throws IOException {
    if (buffer.remaining() >= Long.BYTES) {
      long r = buffer.getLong();
      mPos += Long.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      long r = buffer.getLong();
      mPos += Long.BYTES;
      return r;
    }
  }

  @Override
  public double readDouble() throws IOException {
    if (buffer.remaining() >= Double.BYTES) {
      double r = buffer.getDouble();
      mPos += Double.BYTES;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();
      double r = buffer.getDouble();
      mPos += Double.BYTES;
      return r;
    }
  }

  @Override
  public String readString() throws IOException {
    if (buffer.remaining() >= Integer.BYTES) {
      int size = buffer.getInt();
      String r;
      byte[] bytes = new byte[size];
      if (buffer.remaining() >= size) {
        buffer.get(bytes);
        r = convert2String(bytes);
      } else {
        buffer.compact();
        is.read(b, 0, buffer.remaining());
        buffer.put(b, 0, buffer.remaining());
        buffer.flip();
        buffer.get(bytes);
        r = convert2String(bytes);
      }
      mPos += Integer.BYTES;
      mPos += size;
      return r;
    } else {
      buffer.compact();
      is.read(b, 0, buffer.remaining());
      buffer.put(b, 0, buffer.remaining());
      buffer.flip();

      int size = buffer.getInt();
      byte[] bytes = new byte[size];
      buffer.get(bytes);
      String r = convert2String(bytes);
      mPos += Integer.BYTES;
      mPos += size;
      return r;
    }
  }

  @Override
  public void close() throws IOException {

    if (buffer != null) {
      ((DirectBuffer) buffer).cleaner().clean();
      buffer = null;
    }
  }

  public final void seek(long n) throws IOException {
    throw new SeekUnsupportedException();
  }

  public final void skip(int n) throws IOException {
    int remain = buffer.remaining();
    if (n <= remain) {
      buffer.position(buffer.position() + n);
    } else {
      buffer.position(buffer.limit());
      is.skip((long) (n - remain));
    }
    mPos += n;
  }

  @Override
  public int readByte(int pos) throws IOException {
    throw new SeekUnsupportedException();
  }

  @Override
  public boolean readBool(int pos) throws IOException {

    throw new SeekUnsupportedException();

  }

  @Override
  public int readShort(int pos) throws IOException {
    throw new SeekUnsupportedException();

  }

  @Override
  public int readInt(int pos) throws IOException {

    throw new SeekUnsupportedException();

  }

  @Override
  public float readFloat(int pos) throws IOException {

    throw new SeekUnsupportedException();

  }

  @Override
  public long readLong(int pos) throws IOException {

    throw new SeekUnsupportedException();

  }

  @Override
  public double readDouble(int pos) throws IOException {
    throw new SeekUnsupportedException();

  }

  @Override
  public String readString(int pos) throws IOException {
    throw new UnsupportedEncodingException();
  }

  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    throw new SeekUnsupportedException();

  }


}
