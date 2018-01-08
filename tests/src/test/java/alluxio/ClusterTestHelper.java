package alluxio;

import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AlluxioWorkerService;

import java.util.ArrayList;
import java.util.List;

/**
 * This class created on 2018/1/8.
 *
 * @author Connery
 */
public class ClusterTestHelper {
  protected List<WorkerNetAddress> sWorkerNetAddressList = new ArrayList<>();
  protected LocalAlluxioClusterResource resource;
  protected List<AlluxioWorkerService> workerServiceList;

  public ClusterTestHelper(LocalAlluxioClusterResource resource) {
    this.resource = resource;
    workerServiceList = resource.get().getWorkers();
    for (AlluxioWorkerService service : workerServiceList) {
      sWorkerNetAddressList.add(service.getAddress());
    }
  }

  public LocalAlluxioClusterResource getResource() {
    return resource;
  }

  public ClusterTestHelper(int numberWorker) {
    this(new LocalAlluxioClusterResource.Builder().setNumWorkers(numberWorker).build());
  }

  public void stopWorker(int workerIndex) throws Exception {
    workerServiceList.get(workerIndex).stop();
  }

  public WorkerNetAddress getWorkerAddress(int workerIndex) {
    return sWorkerNetAddressList.get(workerIndex);
  }
}
