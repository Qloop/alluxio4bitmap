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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.Input;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to read a block from Alluxio. An instance extending this class can be
 * obtained by calling {@link AlluxioBlockStore#getInStream}. The buffer size of the stream can be
 * set through configuration. Multiple {@link BufferedBlockInStreamV2}s can be opened for a block.
 * <p>
 * This class provides the same methods as a Java {@link InputStream} with additional methods from
 * Alluxio Stream interfaces.
 *
 * 对一块文件进行整块的内存map
 */
@NotThreadSafe
public abstract class BufferedBlockInStreamV2 extends BlockInStream implements Input {

  /**
   * The id of the block to which this instream provides access.
   */
  protected final long mBlockId;
  /**
   * The size in bytes of the block.
   */
  protected final long mBlockSize;
  /**
   * Internal buffer to improve small read performance.
   */
  protected ByteBuffer mBuffer;
  /**
   * Flag indicating if the stream is closed, can only go from false to true.
   */
  protected boolean mClosed;
  /**
   * Flag indicating if the stream is read.
   */
  protected boolean mBlockIsRead;


  /**
   * Basic constructor for a {@link BufferedBlockInStreamV2}. This sets the necessary variables and
   * creates the initial buffer which is empty and invalid.
   *
   * @param blockId block id for this stream
   * @param blockSize size of the block in bytes
   */
  // TODO(calvin): Get the block lock here when the remote instream locks at a stream level
  public BufferedBlockInStreamV2(long blockId, long blockSize) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    Preconditions.checkArgument(mBlockSize < (long) Integer.MAX_VALUE,
        "the block size:%s is too large than int max");
    mBuffer = allocateBuffer();
    mClosed = false;
    mBlockIsRead = false;
  }

  public static String toString(byte[] b) {
    if (b.length == 0) {
      return null;
    }
    try {
      return new String(b, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (remaining() == 0) {
      close();
      return -1;
    }

    return BufferUtils.byteToInt(mBuffer.get());
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (!(b != null)) {
      Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    }
    if (!(off >= 0 && len >= 0 && len + off <= b.length)) {
      Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
          PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    }
    if (len == 0) {
      return 0;
    } else if (remaining() == 0) { // End of block
      return -1;
    }

    int toRead = Math.min(len, mBuffer.remaining());
    mBuffer.get(b, off, toRead);
    return toRead;

  }

  @Override
  public long remaining() {
    return mBuffer.remaining();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (pos == mBuffer.position()) {
      return;
    }
    mBuffer.position((int) pos);
  }

  @Override
  public long skip(long n) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (n <= 0) {
      return 0;
    }
    int toSkip = (int) Math.min(mBuffer.remaining(), n);
    mBuffer.position(mBuffer.position() + toSkip);
    return toSkip;
  }


  /**
   * Reads from the data source into the buffer. The buffer should be at position 0 and have len
   * valid bytes available after this method is called. This method should not modify mBufferPos,
   * mPos, or increment any metrics.
   *
   * @param len length of data to fill in the buffer, must always be <= buffer size
   * @throws IOException if the read failed to buffer the requested number of bytes
   */
  protected abstract void bufferedRead(int len) throws IOException;

  /**
   * Directly reads data to the given byte array. The data will not go through the internal buffer.
   * This method should not modify mPos or update any metrics collection for bytes read.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array must always be valid within the block
   * @return the number of bytes successfully read
   * @throws IOException if an error occurs reading the data
   */
  protected abstract int directRead(byte[] b, int off, int len) throws IOException;

  /**
   * Increments the number of bytes read metric. Inheriting classes should implement this to
   * increment the correct metric.
   *
   * @param bytes number of bytes to record as read
   */
  protected abstract void incrementBytesReadMetric(int bytes);

  /**
   * Initializes the internal buffer based on the user's specified size. Any reads above half
   * this size will not be buffered.
   *
   * @return a heap buffer of user configured size
   */
  private ByteBuffer allocateBuffer() {
    return ByteBuffer.allocate(
        (int) Configuration.getBytes(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES));
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
  }

  /**
   * Updates the buffer so it is ready to be read from. After calling this method, the buffer will
   * be positioned at 0 and mBufferIsValid will be true. Inheriting classes should implement
   * {@link #bufferedRead(int)} for their read specific logic.
   *
   * @throws IOException if an error occurs reading the data
   */
  protected void updateBuffer() throws IOException {
    int toRead = (int) mBlockSize;
    bufferedRead(toRead);
    Preconditions.checkState(mBuffer.remaining() == toRead);
    incrementBytesReadMetric(toRead);
  }

  @Override
  public int readByte() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return read();
  }

  @Override
  public boolean readBool() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return readByte() == 1;
  }

  @Override
  public int readShort() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getShort() & 0xffff;
  }

  @Override
  public int readInt() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getInt();
  }

  @Override
  public float readFloat() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getFloat();
  }

  @Override
  public long readLong() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getLong();
  }

  @Override
  public double readDouble() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getDouble();
  }

  @Override
  public String readString() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int size = mBuffer.getInt();
    byte[] bytes = new byte[size];
    mBuffer.get(bytes);
    return BufferedBlockInStreamV2.toString(bytes);
  }


  @Override
  public int readByte(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.get(pos) & 0xFF;
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.get(pos) == 1;
  }

  @Override
  public int readShort(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getShort(pos) & 0xffff;
  }

  @Override
  public int readInt(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getInt(pos);
  }

  @Override
  public float readFloat(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getFloat(pos);
  }

  @Override
  public long readLong(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getLong(pos);
  }

  @Override
  public double readDouble(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    return mBuffer.getDouble(pos);
  }

  /**
   * @return the current position of the stream
   */
  protected long getPosition() {
    return mBuffer.position();
  }

  @Override
  public String readString(int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int size = mBuffer.getInt(pos);
    byte[] bytes = new byte[size];
    int tPos = mBuffer.position();
    mBuffer.position(pos + 4);
    mBuffer.get(bytes);
    mBuffer.position(tPos);
    return BufferedBlockInStreamV2.toString(bytes);
  }

  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int tPos = mBuffer.position();
    mBuffer.position(pos);
    mBuffer.get(bytes);
    mBuffer.position(tPos);
  }
}
