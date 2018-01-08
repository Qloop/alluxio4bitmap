package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class created on 2018/1/4.
 *
 * @author Connery
 */
public class AlluxioWriteSpeed {
  private final static long DEFAULT_BUFFER_SIZE = 10_000_000_00l;

  @Test
  @Ignore
  public void testWrite() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES, DEFAULT_BUFFER_SIZE);

    FileSystem fs = FileSystem.Factory.get();//get fileSystem
    FileOutStream fos = null;
    AlluxioURI uri = new AlluxioURI("/AlluxioWriteSpeed.txt");
    if (fs.exists(uri)) {
      fs.delete(uri);
    }
    CreateFileOptions options = CreateFileOptions.defaults();
    options.setBlockSizeBytes(DEFAULT_BUFFER_SIZE);//set block size,if enough big cant leave big data
    options.setWriteType(WriteType.ASYNC_THROUGH);//set storage to memmory
    fos = fs.createFile(uri, options);
    long start = System.currentTimeMillis();
    for (int a = 0; a < DEFAULT_BUFFER_SIZE; a++) {
      fos.write(a % 2 == 0 ? 'a' : 'b');
    }
    fos.flush();
    fos.close();
    System.out.printf("total time:" + (System.currentTimeMillis() - start));

  }
}
