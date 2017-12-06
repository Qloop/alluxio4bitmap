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
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to read a block from Alluxio. An instance extending this class can be
 * obtained by calling {@link AlluxioBlockStore#getInStream}. The buffer size of the stream can be
 * set through configuration. Multiple {@link BufferedBlockInStreamV3}s can be opened for a block.
 * <p>
 * This class provides the same methods as a Java {@link InputStream} with additional methods from
 * Alluxio Stream interfaces.
 * 对一个Block进行的分段进行内存map
 */
@NotThreadSafe
public abstract class BufferedBlockInStreamV3 extends BlockInStream implements Input {

  protected static final int BUFFER_SIZE = (int) Configuration
      .getBytes(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
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
   * Current position of the stream, relative to the start of the block.
   */
  protected long mPos;
  protected long mBufferPosStart;
  protected long mBufferPosEnd;
  /**
   * Flag indicating if the buffer has valid data.
   */
  private boolean mBufferIsValid;

  /**
   * Basic constructor for a {@link BufferedBlockInStreamV3}. This sets the necessary variables and
   * creates the initial buffer which is empty and invalid.
   *
   * @param blockId block id for this stream
   * @param blockSize size of the block in bytes
   */
  // TODO(calvin): Get the block lock here when the remote instream locks at a stream level
  public BufferedBlockInStreamV3(long blockId, long blockSize) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mBufferIsValid = false; // No data in buffer
    mClosed = false;
    mBlockIsRead = false;
    mBuffer = allocateBuffer();

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
    if (mBlockSize - mPos == 0) {
      close();
      return -1;
    }
    if (!mBufferIsValid || mBuffer.remaining() == 0) {
      updateBuffer();
    }
    mPos++;
    mBlockIsRead = true;
    return BufferUtils.byteToInt(mBuffer.get());
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (mClosed) {
      return -1;
    }
    if (b == null) {
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

    int toRead = (int) Math.min(len, remaining());
    if (mBufferIsValid && mBuffer.remaining() >= toRead) { // data is fully contained in the buffer
      mBuffer.get(b, off, toRead);
      mPos += toRead;
      return toRead;
    }

    if (toRead > mBuffer.capacity() / 2) { // directly read if request is > one-half buffer size
      mBufferIsValid = false;
      int bytesRead = directRead(b, off, toRead);
      mPos += bytesRead;
      return bytesRead;
    }

    // For a read <= half the buffer size, fill the buffer first, then read from the buffer.
    updateBuffer();
    mBuffer.get(b, off, toRead);
    mPos += toRead;
    return toRead;
  }

  @Override
  public long remaining() {
    return mBlockSize - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (!(pos >= 0)) {
      Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    }
    if (!(pos <= mBlockSize)) {
      Preconditions.checkArgument(pos <= mBlockSize,
          PreconditionMessage.ERR_SEEK_PAST_END_OF_BLOCK.toString(), mBlockSize);
    }
    mPos = pos;
    if (mPos < mBufferPosStart || mPos >= mBufferPosEnd) {
      mBufferIsValid = false;
    } else {
      mBuffer.position((int) (mPos - mBufferPosStart));
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(mBlockSize - mPos, n);
    if (mBuffer.remaining() > toSkip) {
      mBuffer.position(mBuffer.position() + (int) toSkip);
    } else {
      mBufferIsValid = false;
    }
    mPos += toSkip;
    return toSkip;
  }

  /**
   * @return the current position of the stream
   */
  protected long getPosition() {
    return mPos;
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
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
  }

  /**
   * Updates the buffer so it is ready to be read from. After calling this method, the buffer will
   * be positioned at 0 and mBufferIsValid will be true. Inheriting classes should implement
   * {@link #bufferedRead(int)} for their read specific logic.
   *
   * @throws IOException if an error occurs reading the data
   */
  protected void updateBuffer() throws IOException {
    int toRead = (int) Math.min(BUFFER_SIZE, mBlockSize - mPos);
    bufferedRead(toRead);
    if (!(mBuffer.remaining() == toRead)) {
      Preconditions.checkState(mBuffer.remaining() == toRead);
    }
    mBufferIsValid = true;
  }

  @Override
  public int readByte() throws IOException {
    return read();
  }

  @Override
  public boolean readBool() throws IOException {
    return readByte() == 1;
  }

  @Override
  public int readShort() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (mBufferIsValid && mBuffer.remaining() >= Short.BYTES) {
      int result = mBuffer.getShort() & 0xffff;
      mPos += Short.BYTES;
      return result;
    } else {
      updateBuffer();
      int result = mBuffer.getShort() & 0xffff;
      mPos += Short.BYTES;
      return result;
    }
  }

  @Override
  public int readInt() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (mBufferIsValid && mBuffer.remaining() >= Integer.BYTES) {
      int result = mBuffer.getInt();
      mPos += Integer.BYTES;
      return result;
    } else {
      updateBuffer();
      int result = mBuffer.getInt();
      mPos += Integer.BYTES;
      return result;
    }
  }

  @Override
  public float readFloat() throws IOException {
    if (mBufferIsValid && mBuffer.remaining() >= Float.BYTES) {
      float r = mBuffer.getFloat();
      mPos += Float.BYTES;
      return r;
    } else {
      updateBuffer();
      float r = mBuffer.getFloat();
      mPos += Float.BYTES;
      return r;
    }
  }

  @Override
  public long readLong() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (mBufferIsValid && mBuffer.remaining() >= Long.BYTES) {
      long r = mBuffer.getLong();
      mPos += Long.BYTES;
      return r;
    } else {
      updateBuffer();
      long r = mBuffer.getLong();
      mPos += Long.BYTES;
      return r;
    }
  }

  @Override
  public double readDouble() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    if (mBufferIsValid && mBuffer.remaining() >= Double.BYTES) {
      double r = mBuffer.getDouble();
      mPos += Double.BYTES;
      return r;
    } else {
      updateBuffer();
      double r = mBuffer.getDouble();
      mPos += Double.BYTES;
      return r;
    }
  }

  @Override
  public String readString() throws IOException {
    if (mClosed) {
      Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
    }
    int size = readInt();
    byte[] bytes = new byte[size];
    read(bytes);
    return BufferedBlockInStreamV2.toString(bytes);
  }

  @Override
  public int readByte(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    int result = readByte();
    seek(tPos);
    return result;
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    boolean result = readBool();
    seek(tPos);
    return result;
  }

  @Override
  public int readShort(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    int result = readShort();
    seek(tPos);
    return result;

  }

  @Override
  public int readInt(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    int result = readInt();
    seek(tPos);
    return result;
  }

  @Override
  public float readFloat(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    float result = readFloat();
    seek(tPos);
    return result;
  }

  @Override
  public long readLong(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    long result = readLong();
    seek(tPos);
    return result;
  }

  @Override
  public double readDouble(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    double result = readDouble();
    seek(tPos);
    return result;
  }

  @Override
  public String readString(int pos) throws IOException {
    long tPos = mPos;
    seek(pos);
    String result = readString();
    seek(tPos);
    return result;
  }
}
