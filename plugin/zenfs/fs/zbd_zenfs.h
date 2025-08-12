// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <queue> // CZ
#include <functional> // CZ
#include <map> // CZ
#include <chrono> // CZ

#include <iostream> // CZ
#include "db/db_impl/db_impl.h" // CZ
#include "db/version_edit.h" // CZ

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class ZenFS; // CZ
class DBImpl; // CZ
class Zone; // CZ
class ZoneFile; // CZ
class ZoneExtent; // CZ

class ZonedBlockDevice;
class ZonedBlockDeviceBackend;
class ZoneSnapshot;
class ZenFSSnapshotOptions;

//(ZC)::class and struct added for Zone Cleaning 
struct ZoneExtentInfo {

  ZoneExtent* extent_;
  ZoneFile* zone_file_;
  bool valid_;
  uint32_t length_;
  uint64_t start_;
  Zone* zone_;
  std::string fname_;
  Env::WriteLifeTimeHint lt_;
  int level_;

  explicit ZoneExtentInfo(ZoneExtent* extent, ZoneFile* zone_file, bool valid, 
                          uint64_t length, uint64_t start, Zone* zone, std::string fname, 
                          Env::WriteLifeTimeHint lt, int level)
      : extent_(extent),
        zone_file_(zone_file), 
        valid_(valid), 
        length_(length), 
        start_(start), 
        zone_(zone), 
        fname_(fname), 
        lt_(lt),
        level_(level){ };
  
  void invalidate() {
    assert(extent_ != nullptr);
    if (!valid_) {
      fprintf(stderr, "Try to invalidate invalid extent!\n");
    }       
    valid_ = false;
  };
};

class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  Env::WriteLifeTimeHint min_lifetime_;  
  std::atomic<uint64_t> used_capacity_;
  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  
  std::vector<ZoneExtentInfo *> extent_info_; //CZ
  int zone_id_; //CZ
  void Invalidate(ZoneExtent* extent, ZoneFile* zoneFile); //CZ
  void tracking_where_realExtentInfo(ZoneExtent* extent, ZoneFile* zoneFile); //CZ
  int GetIOZoneID() { return start_/2147483648; }

  void PushExtentInfo(ZoneExtentInfo* extent_info) { 
    extent_info_.push_back(extent_info);
  };

  void EraseExtentInfo(ZoneExtent* extent) { 
    // std::cout << "EraseExtentInfo " << extent << "\n";
    for(const auto ex : extent_info_) {
      if (ex->extent_ == extent) {
        ex->invalidate();
      }
    }
  };

  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  inline IOStatus CheckRelease();
};

class ZonedBlockDeviceBackend {
 public:
  uint32_t block_sz_ = 0;
  uint64_t zone_sz_ = 0;
  uint32_t nr_zones_ = 0;
  uint64_t zone_capacity_ = 0; // jy
  
 public:
  virtual IOStatus Open(bool readonly, bool exclusive,
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint32_t size, uint64_t pos) = 0;
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint32_t GetBlockSize() { return block_sz_; };
  uint64_t GetZoneSize() { return zone_sz_; };
  uint32_t GetNrZones() { return nr_zones_; };
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 private:

  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<Zone *> io_zones;
  std::vector<Zone *> meta_zones;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};

  
  bool logging_mode;
  //for wait-count when if lifetime
  std::atomic<int64_t> short_cnt_{0};
  std::atomic<int64_t> medium_cnt_{0};
  std::atomic<int64_t> long_cnt_{0};
  std::atomic<int64_t> extreme_cnt_{0};

  std::atomic<int64_t> n_fixed_zones_[10]{0}; // for static zone allocation

  //for wait-time
  std::atomic<int64_t> short_accumulated_{0}; //2
  std::atomic<int64_t> medium_accumulated_{0}; //3
  std::atomic<int64_t> long_accumulated_{0}; //4
  std::atomic<int64_t> extreme_accumulated_{0}; //5
  std::atomic<int64_t> total_blocking_time_{0};
  Env* env_;

  std::mutex level_mtx_[4000];

  // std::atomic<long> active_io_zones_;
  // std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  std::atomic<bool> migrating_{false};

  // unsigned int max_nr_active_io_zones_;
  // unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  std::atomic<int64_t> howtoAlloc[10]{0};
  int called_AllocateIOZone = 0;
  int called_AllocateNewZone = 0;
  int called_FinishCheapestIOZone = 0;
  std::atomic<int> called_ResetUnusedIOZones{0};
    std::atomic<uint64_t> finish_elapsed_atomic{0};
    std::atomic<uint32_t> finish_cnt_atomic{0};

  std::atomic<uint32_t> reset_etc{0};
  std::atomic<uint32_t> early_finish_{0}; // ZenFS, TO-ZenFS early zone finish count
  int to_finished{0};
  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  
  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::atomic<uint64_t> alloc_count_{0};
  std::atomic<uint64_t> alloc_time_{0};
  std::atomic<int64_t> diff_cnt_[200]{0};

  unsigned int zone_alloc_mode; // 1: default , 2: CAZA (modified by DE)
  DBImpl* db_ptr_;
  void SetDBPointer(DBImpl* db);
  std::map<uint64_t, std::vector<int>> sst_to_zone_;
  std::map<int, Zone*> id_to_zone_;
  std::mutex sst_zone_mtx_;
  std::map<uint64_t, std::shared_ptr<ZoneFile>> files_;
  std::mutex files_mtx_;
  std::mutex zone_cleaning_mtx;

  std::condition_variable gc_resource_;
  std::mutex gc_zone_mtx_;
  std::atomic<bool> zone_gc_doing{false};

  std::mutex io_zones_mtx;
  std::atomic<bool> io_doing{false};

  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type, Zone **out_zone, ZoneFile* zoneFile);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();
  uint64_t GetLastZoneNum();

  uint64_t GetBlockingTime(int lifetime);
  uint64_t GetOccupiedZoneNum(int lifetime);
  uint64_t GetMaxIOZones();

  std::string GetFilename();
  uint32_t GetBlockSize();

  IOStatus ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  uint64_t GetZoneSize();
  uint32_t GetNrZones();
  std::vector<Zone *> GetMetaZones() { return meta_zones; }
  std::vector<Zone *> GetIOZones() { return io_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);

  IOStatus ReleaseMigrateZone(Zone *zone);
  // IOStatus ZonedBlockDevice::AllocateZoneForCleaning(Zone **out_zone);
  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity, ZoneFile* zoneFile);

  void AddBytesWritten(uint64_t written) { bytes_written_ += written; };
  void AddGCBytesWritten(uint64_t written) { gc_bytes_written_ += written; };
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };

  void PrintVictimInformation(const Zone*, bool = true); //CZ
  void printZoneExtentInfo(std::vector<ZoneExtentInfo *>, bool = true); //CZ
  void printZoneExtent(std::vector<ZoneExtent*> list); //CZ

  void ZoneUtilization();
  uint64_t GetZoneCapacity();

 private:
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, Zone **zone_out, uint32_t min_capacity = 0);
  
  IOStatus GetBestCAZAMatch(Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, int *new_out, Zone **out_zone, InternalKey smallest, InternalKey largest, int level, uint32_t min_capacity = 0);
  IOStatus findOverlappedSST(std::vector<Zone*>& candidates, std::vector<uint64_t>& fno_list, Zone **zone_out, uint32_t min_capacity);
  IOStatus AllocateMostL0Files(const std::set<int>&, Zone **zone_out,  uint32_t min_capacity);
  Zone * AllocateZoneWithSameLevelFiles(const std::vector<uint64_t>&, const InternalKey, const InternalKey, uint32_t min_capacity);
  void SameLevelFileList(const int, std::vector<uint64_t>&); 
  void AdjacentFileList(const InternalKey&, const InternalKey&, const int, std::vector<uint64_t>&);
  IOStatus AllocateEmptyZone(Zone **zone_out);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
