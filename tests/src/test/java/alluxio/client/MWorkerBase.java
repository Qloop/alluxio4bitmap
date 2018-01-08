package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.ClusterTestHelper;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.security.authorization.Mode;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AlluxioWorkerService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;

/**
 * This class created on 2018/1/8.
 *
 * @author Connery
 */
@SuppressWarnings("Duplicates")
public class MWorkerBase {
  private static final int BLOCK_SIZE = 30;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = BLOCK_SIZE * 4 + 1;
  private static final int DELTA = BLOCK_SIZE / 2;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
          new LocalAlluxioClusterResource.Builder().setNumWorkers(3).build();
  private static FileSystem sFileSystem = null;
  private static CreateFileOptions sWriteBoth;
  private static CreateFileOptions sWriteAlluxio;
  private static CreateFileOptions sWriteUnderStore;
  private static String sTestPath;
  private static ClusterTestHelper helper = new ClusterTestHelper(sLocalAlluxioClusterResource);
  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteBoth = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
            .setWriteType(WriteType.CACHE_THROUGH);
    sWriteAlluxio = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
            .setWriteType(WriteType.MUST_CACHE);
    sWriteUnderStore = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
            .setWriteType(WriteType.THROUGH);
    sTestPath = PathUtils.uniqPath();

    // Create files of varying size and write type to later read from
//    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
//      for (CreateFileOptions op : getOptionSet()) {
//        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());
//        FileSystemTestUtils.createByteFile(sFileSystem, path, op, k);
//      }
//    }
    List<AlluxioWorkerService> s = sLocalAlluxioClusterResource.get().getWorkers();


  }

  private static List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteAlluxio);
    ret.add(sWriteUnderStore);
    return ret;
  }

  /**
   * Tests {@link FileInStreamV2#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws Exception {
    List<AlluxioWorkerService> s = sLocalAlluxioClusterResource.get().getWorkers();
    for (AlluxioWorkerService service : s) {
      System.out.println(service.getAddress().toString());
      service.waitForReady();
      service.stop();
    }


//    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
//      for (CreateFileOptions op : getOptionSet()) {
//        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
//        AlluxioURI uri = new AlluxioURI(filename);
//
//        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
//        byte[] ret = new byte[k / 2];
//        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
//        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
//        is.close();
//
//        is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
//        ret = new byte[k];
//        Assert.assertEquals(k, is.read(ret, 0, k));
//        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
//        is.close();
//      }
//    }


  }

  @Test
  public void writeSpecificWorker() throws Exception {
    AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + 0 + "_" + sWriteUnderStore.hashCode());
    sWriteAlluxio.setLocationPolicy(new FileWriteLocationPolicy() {

      @Override
      public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
        return helper.getWorkerAddress(1);
      }
    });
    FileSystemTestUtils.createByteFile(sFileSystem, path, sWriteAlluxio, 1000);

    URIStatus status = sFileSystem.getStatus(path);
    Assert.assertTrue(status.getFileBlockInfos().size() > 0);
    for (FileBlockInfo blockInfo : status.getFileBlockInfos()) {
      for (BlockLocation location : blockInfo.getBlockInfo().getLocations())
        Assert.assertEquals(location.getWorkerAddress(), helper.getWorkerAddress(1));
    }
  }

  @Test
  public void writeWorkerAndClose() throws Exception {
    AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + 0 + "_" + sWriteUnderStore.hashCode());
    sWriteAlluxio.setLocationPolicy(new FileWriteLocationPolicy() {
      @Override
      public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
        return helper.getWorkerAddress(1);
      }
    });
    FileSystemTestUtils.createByteFile(sFileSystem, path, sWriteBoth, 1000);

    URIStatus status = sFileSystem.getStatus(path);
    Assert.assertTrue(status.getFileBlockInfos().size() > 0);
    for (FileBlockInfo i : status.getFileBlockInfos()) {
      for (BlockLocation location : i.getBlockInfo().getLocations())
        Assert.assertEquals(location.getWorkerAddress(), helper.getWorkerAddress(1));
    }
    helper.stopWorker(1);
    sFileSystem.openFile(path);
  }
}

