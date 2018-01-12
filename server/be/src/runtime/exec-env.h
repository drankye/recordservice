// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "common/status.h"
#include "exec/scannerLock.h"
#include "exprs/timestamp-functions.h"
#include "runtime/client-cache.h"
#include "util/cgroups-mgr.h"
#include "util/hdfs-bulk-ops.h" // For declaration of HdfsOpThreadPool
#include "resourcebroker/resource-broker.h"

namespace impala {

class Catalog;
class DataStreamMgr;
class DiskIoMgr;
class HBaseTableFactory;
class HdfsFsCache;
class LibCache;
class Scheduler;
class StatestoreSubscriber;
class TestExecEnv;
class Webserver;
class MetricGroup;
class MemTracker;
class ThreadResourceMgr;
class CgroupsManager;
class ImpalaServer;
class RequestPoolService;
class Frontend;
class LockTracker;
class TmpFileMgr;

/// Execution environment for queries/plan fragments.
/// Contains all required global structures, and handles to
/// singleton services. Clients must call StartServices exactly
/// once to properly initialise service state.
class ExecEnv {
 public:
  // Id is the server id that needs to be unique across the cluster.
  ExecEnv(const std::string& server_id, bool running_planner = false,
      bool running_worker = false);

  // Ctor used for tests.
  ExecEnv(const std::string& hostname, int backend_port, int subscriber_port,
      int webserver_port, const std::string& statestore_host, int statestore_port);

  /// Returns the first created exec env instance. In a normal impalad, this is
  /// the only instance. In test setups with multiple ExecEnv's per process,
  /// we return the first instance.
  static ExecEnv* GetInstance() { return exec_env_; }

  /// Empty destructor because the compiler-generated one requires full
  /// declarations for classes in scoped_ptrs.
  virtual ~ExecEnv();

  void SetImpalaServer(ImpalaServer* server) { impala_server_ = server; }

  StatestoreSubscriber* statestore_subscriber() {
    return statestore_subscriber_.get();
  }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  ImpalaInternalServiceClientCache* impalad_client_cache() {
    return impalad_client_cache_.get();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return catalogd_client_cache_.get();
  }
  const std::string& server_id() const { return server_id_; }
  HBaseTableFactory* htable_factory() { return htable_factory_.get(); }
  DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  MetricGroup* metrics() { return metrics_.get(); }
  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  ThreadResourceMgr* thread_mgr() { return thread_mgr_.get(); }
  CgroupsMgr* cgroups_mgr() { return cgroups_mgr_.get(); }
  HdfsOpThreadPool* hdfs_op_thread_pool() { return hdfs_op_thread_pool_.get(); }
  TmpFileMgr* tmp_file_mgr() { return tmp_file_mgr_.get(); }
  ImpalaServer* impala_server() { return impala_server_; }
  Frontend* frontend() { return frontend_.get(); };
  Catalog* catalog() { return catalog_.get(); };

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

  ResourceBroker* resource_broker() { return resource_broker_.get(); }
  Scheduler* scheduler() { return scheduler_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }

  const TNetworkAddress& backend_address() const { return backend_address_; }

  /// Starts any dependent services in their correct order
  virtual Status StartServices();

  /// Initializes the exec env for running FE tests.
  Status InitForFeTests();

  /// Returns true if this environment was created from the FE tests. This makes the
  /// environment special since the JVM is started first and libraries are loaded
  /// differently.
  bool is_fe_tests() { return is_fe_tests_; }

  /// Returns true if this daemon is recordserviced.
  bool is_record_service() const { return running_planner_ || running_worker_; }

  bool running_planner() const { return running_planner_; }
  bool running_worker() const { return running_worker_; }

  /// Returns true if the Llama in use is pseudo-distributed, used for development
  /// purposes. The pseudo-distributed version has special requirements for specifying
  /// resource locations.
  bool is_pseudo_distributed_llama() { return is_pseudo_distributed_llama_; }

  /// Returns the scanner lock shared by all scanner threads.
  ScannerLock* get_shared_scanner_lock() { return &shared_scanner_lock_; }

  /// Returns the configured defaultFs set in core-site.xml
  string default_fs() { return default_fs_; }

 protected:
  /// True if this daemon is running the planner/worker service. If either is true
  /// this must be the recordserviced.
  const bool running_planner_;
  const bool running_worker_;

  /// ID for this process that needs to be unique across all instances in each of
  /// the services this daemon is part of. This ID is used for membership registration
  /// and must be able to uniquely identify a server.
  const std::string server_id_;

  /// Leave protected so that subclasses can override
  boost::scoped_ptr<LockTracker> lock_tracker_;
  boost::scoped_ptr<DataStreamMgr> stream_mgr_;
  boost::scoped_ptr<ResourceBroker> resource_broker_;
  boost::scoped_ptr<Scheduler> scheduler_;
  boost::scoped_ptr<StatestoreSubscriber> statestore_subscriber_;
  boost::scoped_ptr<ImpalaInternalServiceClientCache> impalad_client_cache_;
  boost::scoped_ptr<CatalogServiceClientCache> catalogd_client_cache_;
  boost::scoped_ptr<HBaseTableFactory> htable_factory_;
  boost::scoped_ptr<DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<MetricGroup> metrics_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<ThreadResourceMgr> thread_mgr_;
  boost::scoped_ptr<CgroupsMgr> cgroups_mgr_;
  boost::scoped_ptr<HdfsOpThreadPool> hdfs_op_thread_pool_;
  boost::scoped_ptr<TmpFileMgr> tmp_file_mgr_;
  boost::scoped_ptr<RequestPoolService> request_pool_service_;
  boost::scoped_ptr<Frontend> frontend_;

  // Only set if this is recordserviced for the in-process catalog instance.
  boost::scoped_ptr<Catalog> catalog_;

  /// Not owned by this class
  ImpalaServer* impala_server_;

  bool enable_webserver_;

 private:
  static ExecEnv* exec_env_;
  TimezoneDatabase tz_database_;
  bool is_fe_tests_;

  /// Address of the Impala backend server instance
  TNetworkAddress backend_address_;

  /// True if the cluster has set 'yarn.scheduler.include-port-in-node-name' to true,
  /// indicating that this cluster is pseudo-distributed. Should not be true in real
  /// deployments.
  bool is_pseudo_distributed_llama_;

  /// Lock shared by all scanner threads. When memory is insufficient for running all
  /// active scanner threads concurrently, only one scanner thread can acquire this
  /// lock and fetch the new row batch.
  ScannerLock shared_scanner_lock_;

  /// Initializes statestore_subscriber_.
  void InitStatestoreSubscriber();

  /// fs.defaultFs value set in core-site.xml
  std::string default_fs_;

  /// Initialise cgroups manager, detect test RM environment and init resource broker.
  void InitRm();

  // Populates the peer daemons in the document.
  void BackendsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);
};

} // namespace impala

#endif
