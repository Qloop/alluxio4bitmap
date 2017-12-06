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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.FileInputWrapper;
import alluxio.client.block.LocalBlockInStreamV3;
import alluxio.client.block.RemoteBlockInStream;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.block.UnderStoreBlockInStream.UnderStoreStreamFactory;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.block.BlockId;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 * <p>
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 */
@PublicApi
@NotThreadSafe
public class FileInStreamV3 extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);

  /**
   * The instream options.
   */
  private final InStreamOptions mInStreamOptions;

  private final AlluxioBlockStore mBlockStore;


  /**
   * The blockId used in the block streams.
   */
  private long mStreamBlockId;

  /**
   * The read buffer in file seek. This is used in {@link #readCurrentBlockToEnd()}.
   */
  private byte[] mSeekBuffer;
  private Input input;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   */
  protected FileInStreamV3(URIStatus status, InStreamOptions options, FileSystemContext context) {
    super(status, options, context);
    mStatus = status;
    mInStreamOptions = options;
    mClosed = false;
    int seekBufferSizeBytes = Math.max((int) options.getSeekBufferSizeBytes(), 1);
    mSeekBuffer = new byte[seekBufferSizeBytes];
    mBlockStore = AlluxioBlockStore.create(context);
    try {
      updateStreams();
      Preconditions.checkState(mCurrentBlockInStream instanceof Input,
          "block stream in file:%s is:%s should be local mode ",
          mCurrentBlockInStream != null ? mCurrentBlockInStream.getClass() : "null stream",
          mStatus.getPath());
      input = (Input) mCurrentBlockInStream;
    } catch (Exception e) {
      LOG.error("the fileInStream of path:{} failed to update stream", status.getPath());
      throw new RuntimeException(e);
    }
    if (!mStatus.getBlockIds().isEmpty()) {
      Preconditions.checkState(mStatus.getBlockIds().size() == 1,
          "the file:%s must be consisted of one block", mStatus.getPath());
      if (!isReadingFromLocalBlockWorker()) {
        LOG.warn("file:{} should be local mode", mStatus.getPath());
      }
    } else {
      LOG.warn("file:{} own null block", mStatus.getPath());
    }

    LOG.debug("Init FileInStream V3 with options {}", options);
  }

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link FileInStreamV3} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options,
      FileSystemContext context) {
    if (status.getLength() == Constants.UNKNOWN_SIZE) {
      throw new UnsupportedOperationException("FileInStreamV3 not support unknown size file");
    }
    return new FileInStreamV3(status, options, context);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    updateStreams();
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    if (input instanceof FileInputWrapper) {
      input.close();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    int data = mCurrentBlockInStream.read();

    if (data == -1) {
      // The underlying stream is done.
      return -1;
    }

    mPos++;

    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = mCurrentBlockInStream.read(b, off, len);
    if (bytesRead > 0) {
      mPos += bytesRead;
      return bytesRead;
    } else {
      return -1;
    }
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    if (!PACKET_STREAMING_ENABLED) {
      throw new RuntimeException(String.format(
          "Positioned read is not supported, please set %s to true to enable positioned read.",
          PropertyKey.USER_PACKET_STREAMING_ENABLED.toString()));
    }
    if (pos < 0 || pos >= mFileLength) {
      return -1;
    }

    int lenCopy = len;

    while (len > 0) {
      if (pos >= mFileLength) {
        break;
      }
      long blockId = getBlockId(pos);
      long blockPos = pos % mBlockSize;
      try (InputStream inputStream = getBlockInStream(blockId)) {
        assert inputStream instanceof PositionedReadable;
        int bytesRead = ((PositionedReadable) inputStream).positionedRead(blockPos, b, off, len);
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
      }
    }
    return lenCopy - len;
  }

  @Override
  public long remaining() {
    if (!mClosed) {
      /**
       * 当前Block存在，那么就用block的剩余量。
       */
      if (mCurrentBlockInStream != null) {
        return ((BoundedStream) mCurrentBlockInStream).remaining();
      } else {
        /**
         * 当前没有block流存在，那么文件总长度就是剩余量。
         */
        return mFileLength;
      }
    } else {
      return 0;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0 || pos > mFileLength) {
      Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
      Preconditions.checkArgument(pos <= mFileLength,
          PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);
    }

    assert mCurrentBlockInStream instanceof Seekable;
    ((Seekable) mCurrentBlockInStream).seek(pos);

  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    return mCurrentBlockInStream.skip(n);
  }

  /**
   * @return the maximum position to seek to
   */
  protected long maxSeekPosition() {
    return mFileLength;
  }

  /**
   * @param pos the position to check
   * @return the block size in bytes for the given pos, used for worker allocation
   */
  protected long getBlockSizeAllocation(long pos) {
    return getBlockSize(pos);
  }

  /**
   * Creates and returns a {@link InputStream} for the UFS.
   *
   * @param blockStart the offset to start the block from
   * @param length the length of the block
   * @param path the UFS path
   * @return the {@link InputStream} for the UFS
   * @throws IOException if the stream cannot be created
   */
  protected InputStream createUnderStoreBlockInStream(long blockStart, long length, String path)
      throws IOException {
    return new UnderStoreBlockInStream(mContext, blockStart, length, mBlockSize,
        getUnderStoreStreamFactory(path, mContext));
  }

  protected UnderStoreStreamFactory getUnderStoreStreamFactory(String path, FileSystemContext
      context) throws IOException {
    if (Configuration.getBoolean(PropertyKey.USER_UFS_DELEGATION_ENABLED)) {
      return new DelegatedUnderStoreStreamFactory(context, path);
    } else {
      return new DirectUnderStoreStreamFactory(path);
    }
  }

  /**
   * If we are not in the last block or if the last block is equal to the normal block size,
   * return the normal block size. Otherwise return the block size of the last block.
   *
   * @param pos the position to get the block size for
   * @return the size of the block that covers pos
   */
  protected long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mFileLength % mBlockSize;
    if (mFileLength - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }

  /**
   * Checks whether block instream and cache outstream should be updated.
   * This function is only called by {@link #updateStreams()}.
   *
   * @param currentBlockId cached result of {@link #getCurrentBlockId()}
   * @return true if the block stream should be updated
   */
  protected boolean shouldUpdateStreams(long currentBlockId) {
    if (mCurrentBlockInStream == null || currentBlockId != mStreamBlockId) {
      return true;
    }
    return inStreamRemaining() == 0;
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   */
  private void closeOrCancelCacheStream() {
    if (mCurrentCacheStream == null) {
      return;
    }
    try {
      if (cacheStreamRemaining() == 0) {
        mCurrentCacheStream.close();
      } else {
        cacheStreamCancel();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof BlockDoesNotExistException) {
        // This happens if two concurrent readers read trying to cache the same block. One cancelled
        // before the other. Then the other reader will see this exception since we only keep
        // one block per blockId in block worker.
        LOG.info("Block {} does not exist when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof InvalidWorkerStateException) {
        // This happens if two concurrent readers trying to cache the same block and they acquired
        // different file system contexts.
        // instances (each instance has its only session ID).
        LOG.info("Block {} has invalid worker state when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof BlockAlreadyExistsException) {
        // This happens if two concurrent readers trying to cache the same block. One successfully
        // committed. The other reader sees this.
        LOG.info("Block {} exists.", getCurrentBlockId());
      } else {
        // This happens when there are any other cache stream close/cancel related errors (e.g.
        // server unreachable due to network partition, server busy due to alluxio worker is
        // busy, timeout due to congested network etc). But we want to proceed since we want
        // the user to continue reading when one Alluxio worker is having trouble.
        LOG.info(
            "Closing or cancelling the cache stream encountered IOExecption {}, reading from the "
                + "regular stream won't be affected.", e.getMessage());
      }
    }
    mCurrentCacheStream = null;
  }

  /**
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    return mStatus.getBlockIds().get(0);
  }

  /**
   * @param pos the pos
   * @return the block ID based on the pos
   */
  private long getBlockId(long pos) {
    int index = (int) (pos / mBlockSize);
    Preconditions
        .checkState(index < mStatus.getBlockIds().size(), PreconditionMessage.ERR_BLOCK_INDEX);
    return mStatus.getBlockIds().get(index);
  }

  /**
   * Handles IO exceptions thrown in response to the worker cache request. Cache stream is closed
   * or cancelled after logging some messages about the exceptions.
   *
   * @param e the exception to handle
   */
  private void handleCacheStreamIOException(IOException e) {
    if (e.getCause() instanceof BlockAlreadyExistsException) {
      // This can happen if there are two readers trying to cache the same block. The first one
      // created the block (either as temp block or committed block). The second sees this
      // exception.
      LOG.info(
          "The block with ID {} is already stored in the target worker, canceling the cache "
              + "request.", getCurrentBlockId());
    } else {
      LOG.warn("The block with ID {} could not be cached into Alluxio storage.",
          getCurrentBlockId());
    }
    closeOrCancelCacheStream();
  }

  /**
   * Only updates {@link #mCurrentCacheStream}, {@link #mCurrentBlockInStream} and
   * {@link #mStreamBlockId} to be in sync with the current block (i.e.
   * {@link #getCurrentBlockId()}).
   * If this method is called multiple times, the subsequent invokes become no-op.
   * Call this function every read and seek unless you are sure about the block streams are
   * up-to-date.
   *
   * @throws IOException if the next cache stream or block stream cannot be created
   */
  protected void updateStreams() throws IOException {
    long currentBlockId = getCurrentBlockId();
    if (shouldUpdateStreams(currentBlockId)) {
      // The following two function handle negative currentBlockId (i.e. the end of file)
      // correctly.
      updateBlockInStream(currentBlockId);
      mStreamBlockId = currentBlockId;
    }
  }


  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with mPos's block. The new block
   * stream created with {@link UnderStoreBlockInStream#mPos} at position 0.
   * This function is only called in {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   * @throws IOException if the next block in stream cannot be obtained
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream = null;
    }

    // blockId = -1 if mPos = EOF.
    if (blockId < 0) {
      return;
    }
    mCurrentBlockInStream = getBlockInStream(blockId);
  }

  /**
   * Gets the block in stream corresponding a block ID.
   *
   * @param blockId the block ID
   * @return the block in stream
   * @throws IOException if the block in stream cannot be obtained
   */
  private InputStream getBlockInStream(long blockId) throws IOException {
    try {
      if (mAlluxioStorageType.isPromote()) {
        try {
          mBlockStore.promote(blockId);
        } catch (IOException e) {
          // Failed to promote.
          LOG.warn("Promotion of block with ID {} failed.", blockId, e);
        }
      }
      return mBlockStore.getInStream(blockId, mInStreamOptions);
    } catch (IOException e) {
      LOG.debug("Failed to get BlockInStream for block with ID {}, using UFS instead. {}", blockId,
          e);
      if (!mStatus.isPersisted()) {
        LOG.error("Could not obtain data for block with ID {} from Alluxio."
            + " The block is also not available in the under storage.", blockId);
        throw e;
      }
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      return createUnderStoreBlockInStream(blockStart, getBlockSize(blockStart),
          mStatus.getUfsPath());
    }
  }


  /**
   * Reads till the file offset (mPos) equals pos or the end of the current block (whichever is
   * met first) if pos > mPos. Otherwise no-op.
   *
   * @param pos file offset
   * @throws IOException if read or cache write fails
   */
  private void readCurrentBlockToPos(long pos) throws IOException {
    Preconditions.checkNotNull(mCurrentBlockInStream);
    Preconditions.checkNotNull(mCurrentCacheStream);
    long len = Math.min(pos - mPos, inStreamRemaining());
    if (len <= 0) {
      return;
    }

    do {
      // Account for the last read which might be less than mSeekBufferSizeBytes bytes.
      int bytesRead = read(mSeekBuffer, 0, (int) Math.min(mSeekBuffer.length, len));
      Preconditions.checkState(bytesRead > 0, PreconditionMessage.ERR_UNEXPECTED_EOF);
      len -= bytesRead;
    } while (len > 0);
  }

  /**
   * @return true if {@code mCurrentBlockInStream} is reading from a local block worker
   */
  private boolean isReadingFromLocalBlockWorker() {
    return (mCurrentBlockInStream instanceof LocalBlockInStreamV3) || (
        mCurrentBlockInStream instanceof Locatable && ((Locatable) mCurrentBlockInStream)
            .isLocal());
  }

  /**
   * @return true if {@code mCurrentBlockInStream} is reading from a remote block worker
   */
  private boolean isReadingFromRemoteBlockWorker() {
    return (mCurrentBlockInStream instanceof RemoteBlockInStream) || (
        mCurrentBlockInStream instanceof Locatable && !(((Locatable) mCurrentBlockInStream)
            .isLocal()));
  }


  /**
   * @return the remaining bytes in the current block in stream
   */
  protected long inStreamRemaining() {
    assert mCurrentBlockInStream instanceof BoundedStream;
    return ((BoundedStream) mCurrentBlockInStream).remaining();
  }

  /**
   * Seeks to the given pos in the current in stream.
   *
   * @param pos the pos
   * @throws IOException if it fails to seek
   */
  private void inStreamSeek(long pos) throws IOException {
    assert mCurrentBlockInStream instanceof Seekable;
    ((Seekable) mCurrentBlockInStream).seek(pos);
  }

  /**
   * @return the remaining bytes in the current cache out stream
   */
  private long cacheStreamRemaining() {
    assert mCurrentCacheStream instanceof BoundedStream;
    return ((BoundedStream) mCurrentCacheStream).remaining();
  }

  /**
   * Cancels the current cache out stream.
   *
   * @throws IOException if it fails to cancel the cache out stream
   */
  private void cacheStreamCancel() throws IOException {
    assert mCurrentCacheStream instanceof Cancelable;
    ((Cancelable) mCurrentCacheStream).cancel();
  }

  @Override
  public int readByte() throws IOException {
    return input.readByte();
  }

  @Override
  public boolean readBool() throws IOException {
    return input.readBool();
  }

  @Override
  public int readShort() throws IOException {
    return input.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return input.readInt();

  }

  @Override
  public float readFloat() throws IOException {
    return input.readFloat();

  }

  @Override
  public long readLong() throws IOException {
    return input.readLong();

  }

  @Override
  public double readDouble() throws IOException {
    return input.readDouble();
  }

  @Override
  public String readString() throws IOException {
    return input.readString();
  }

  @Override
  public int readByte(int pos) throws IOException {
    return input.readByte(pos);
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    return input.readBool(pos);
  }

  @Override
  public int readShort(int pos) throws IOException {
    return input.readShort(pos);
  }

  @Override
  public int readInt(int pos) throws IOException {
    return input.readInt(pos);
  }

  @Override
  public float readFloat(int pos) throws IOException {
    return input.readFloat(pos);
  }

  @Override
  public long readLong(int pos) throws IOException {
    return input.readLong(pos);

  }

  @Override
  public double readDouble(int pos) throws IOException {
    return input.readDouble(pos);
  }

  @Override
  public String readString(int pos) throws IOException {
    return input.readString(pos);
  }
}
