// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <iomanip>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <semaphore>
#include <iostream>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbdlib_zenfs.h"
#include "zonefs_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, unsigned int idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      busy_(false),
      start_(zbd_be->ZoneStart(zones, idx)),
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  zone_id_ = idx;
  if (zbd_be->ZoneIsWritable(zones, idx))
    capacity_ = max_capacity_ - (wp_ - start_);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}
 
IOStatus Zone::Reset() {
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
  assert(IsBusy());

  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  assert(IsBusy());

  IOStatus ios = zbd_be_->Finish(start_);
  if (ios != IOStatus::OK()) return ios;

  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    IOStatus ios = zbd_be_->Close(start_);
    if (ios != IOStatus::OK()) return ios;
  }
  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);
  char *ptr = data;
  uint32_t left = size;
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);
    
    if (ret < 0) {
      return IOStatus::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    zbd_->AddBytesWritten(ret);
  }

  return IOStatus::OK();
}

inline IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize()))
      return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : logger_(logger), metrics_(metrics) {
  if (backend == ZbdBackendType::kBlockDev) {
    zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
    Info(logger_, "New Zoned Block Device: %s", zbd_be_->GetFilename().c_str());
  } else if (backend == ZbdBackendType::kZoneFS) {
    zbd_be_ = std::unique_ptr<ZoneFsBackend>(new ZoneFsBackend(path));
    Info(logger_, "New zonefs backing: %s", zbd_be_->GetFilename().c_str());
  }

  /* 
     @ type 1 : default zenFS
     @ type 2 : CAZA
     
  */
  zone_alloc_mode = 2; 
  
  logging_mode = false;
  env_ = Env::Default();

}

void ZonedBlockDevice::SetDBPointer(DBImpl* db) {
  db_ptr_ = db;
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  std::unique_ptr<ZoneList> zone_rep;
  unsigned int max_nr_active_zones;
  unsigned int max_nr_open_zones;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  // Reserve one zone for metadata and another one for extent migration
  int reserved_zones = 2;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  IOStatus ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones,
                               &max_nr_open_zones);
  if (ios != IOStatus::OK()) return ios;

  if (zbd_be_->GetNrZones() < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned backend (" +
                                  std::to_string(ZENFS_MIN_ZONES) +
                                  " required)");
  }

  if (max_nr_active_zones == 0)
    max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones;

  if (max_nr_open_zones == 0)
    max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       zbd_be_->GetNrZones(), max_nr_active_zones, max_nr_open_zones);

  zone_rep = zbd_be_->ListZones();
  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    Error(logger_, "Failed to list zones");
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < zone_rep->ZoneCount()) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
      }
      m++;
    }
    i++;
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < zone_rep->ZoneCount(); i++) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_be_->ZoneIsActive(zone_rep, i)) {
          active_io_zones_++;
          if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) {
          return status;
        }
      }
    }
  }

  start_time_ = time(NULL);

  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetOccupiedZoneNum(int lifetime) {
  return n_fixed_zones_[lifetime];
}

uint64_t ZonedBlockDevice::GetMaxIOZones(){
  return max_nr_active_io_zones_ - n_fixed_zones_[0] - n_fixed_zones_[2];
}

IOStatus ZonedBlockDevice::AdjustIOZones(int lifetime, int value) {
    level_mtx_[lifetime].lock();

  if (value < 0) {

    Zone *candidate_zone = nullptr;
    IOStatus s;
    int64_t candidate_capacity;
    int used_zone_num = 0;

    for (const auto z : io_zones) {
      if (z->lifetime_ == lifetime && !z->IsFull()) {
        used_zone_num++;
      }
    }


    if (n_fixed_zones_[lifetime] + value >= used_zone_num) {
      n_fixed_zones_[lifetime] +=value;
    } else {
      int target = (value*-1) - (n_fixed_zones_[lifetime] - used_zone_num);

      for (int i=1;i<=target;i++) {
        candidate_capacity = -1;
        candidate_zone = nullptr;

        for (const auto z : io_zones) {
          if(z->lifetime_ == lifetime && !z->IsFull()) {
            if (candidate_capacity == -1 || candidate_capacity > (int64_t)z->GetCapacityLeft()) {
              if (candidate_zone != nullptr) candidate_zone->CheckRelease();
              candidate_zone = z;
              candidate_capacity = z->GetCapacityLeft();
            }
          }
        }
        if (candidate_zone == nullptr) {
          std::cout << "[AdjustIOZones] candidate not found!\n";
          break;
        }
          

        while(!candidate_zone->Acquire());

        if (!candidate_zone->IsFull()) {
          s = candidate_zone->Finish();
          if (!s.ok()) {
            std::cout << "[AdjustIOZones] finish failed\n";
            candidate_zone->Release();
          }
          s = candidate_zone->CheckRelease();
          if (!s.ok()) {
            std::cout << "[AdjustIOZones] release failed\n";
          }
          PutActiveIOZoneToken();
        } 
      }
      n_fixed_zones_[lifetime] +=value;
    }
    

    


  } else {
      n_fixed_zones_[lifetime] +=value;
  }

  level_mtx_[lifetime].unlock();

  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetBlockingTime(int lifetime) {

  uint64_t blocking_time_ = 0;
  if (lifetime == 3) {
    blocking_time_ = medium_accumulated_;
  } else if (lifetime == 4) {
    blocking_time_ = long_accumulated_;
  } else {
    blocking_time_ = extreme_accumulated_;
  }
  return blocking_time_;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate =
        double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    assert(garbage_rate > 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) || (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_COULD_BE_WORSE;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

IOStatus ZonedBlockDevice::ResetUnusedIOZones() {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) { // if wal -> true
  long allocator_open_limit;

  /* Avoid non-priortized allocators from starving prioritized ones */
  if (prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  zone_resources_.notify_one();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
  }
  zone_resources_.notify_one();
}

IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;

  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone() {
  IOStatus s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }

  if (!release_status.ok()) {
    return release_status;
  }

  return s;
}

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;

  for (const auto z : io_zones) {
    if (z->Acquire()) { // if not busy 
      if ((z->used_capacity_ > 0) && !z->IsFull() && z->capacity_ >= min_capacity) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {

          if (allocated_zone != nullptr) { // if not first
            s = allocated_zone->CheckRelease(); // release previous allocated zone
            if (!s.ok()) {
              IOStatus s_ = z->CheckRelease();
              if (!s_.ok()) return s_;
              return s;
            }
          }
          //first allocation {
          allocated_zone = z; // allocate new zone
          best_diff = diff;
          //}
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  
  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::GetBestCAZAMatch(Zone **zone_out, 
                                            InternalKey smallest, InternalKey largest, int level, uint32_t min_capacity){
  // fprintf(stdout, "GetBestCAZAMatch\n");
  Zone *allocated_zone = nullptr;
  IOStatus s;
  // io_zones_mtx.lock();
  
  assert(sst_to_zone_.empty());
  // fprintf(stdout, "sst_to_zone is not empty!\t sst_to_zone : ");
  // for(auto iter = sst_to_zone_.begin() ; iter != sst_to_zone_.end(); iter++){
	// 	std::cout << iter->first << " " ;
	// }
  // std::cout << std::endl; 

  // There's valid SSTables in Zones
  // Find zone where the files located at adjacent level and having overlapping keys
  std::vector<uint64_t> fno_list;
  std::vector<Zone*> candidates;
  AdjacentFileList(smallest, largest, level, fno_list);

  if (!fno_list.empty()) {
    /* There are SSTables with overlapped keys and adjacent level.
        (1) Find the Zone where the SSTables are written */

    std::set<int> zone_list;
    sst_zone_mtx_.lock();
    for (uint64_t fno : fno_list) {
      auto z = sst_to_zone_.find(fno);
      if (z != sst_to_zone_.end()) {
        for (int zone_id : z->second) {
          zone_list.insert(zone_id);
        }
      }
    }
    sst_zone_mtx_.unlock(); 

    // (2) Pick the Zones with free space as candidates
    for (const auto z : io_zones) {
      auto search = zone_list.find(z->zone_id_);
      if (search != zone_list.end()){
        if (!z->IsFull() && !z->IsBusy()) {
          candidates.push_back(z);
        }
      }
    }

    if (!candidates.empty()) {
      /* There is at least one zone having free space */
      // If there's more than one zone, pick the zone with most data with overlapped SST
      uint64_t overlapped_data = 0;
      uint64_t alloc_inval_data = 0;
      for (const auto z : candidates) {
        uint64_t data_amount = 0;
        uint64_t inval_data = 0;
        for (const auto ext : z->extent_info_) {
          if (!ext->valid_) {
            inval_data += ext->length_;
          } else if (ext->valid_ && ext->zone_file_->is_sst_) {
            uint64_t cur_fno = ext->zone_file_->fno_;
            for (uint64_t fno : fno_list) {
                if (cur_fno == fno ) {
                  data_amount += ext->length_; 
                }
            }
          }
          if (data_amount > overlapped_data && !z->IsBusy() && z->capacity_ >= min_capacity) {
            allocated_zone = z;
            alloc_inval_data = inval_data;
          } 
          else if ((data_amount == overlapped_data) && (alloc_inval_data > inval_data) && !z->IsBusy() && z->capacity_ >= min_capacity) {
            allocated_zone = z;
            alloc_inval_data = inval_data;
          }
        }
      }
    }

    if(allocated_zone != nullptr){
      while (!allocated_zone->Acquire());
    }
  } // if (!fno_list.empty())
  else if (fno_list.empty() && (level==0 || level==100 )) {
    /* (1) There is no matching files being overlapped with current file
      ->(TODO)Find the file within the same level which has smallest key diff*/
    
    std::set<int> zone_list;
  }
  else{
    // SameLevelFileList(level, fno_list);
    // allocated_zone = AllocateZoneWithSameLevelFiles(fno_list, smallest, largest);
  }

  *zone_out = allocated_zone;

  // io_zones_mtx.unlock();
  return IOStatus::OK();
} 

void ZonedBlockDevice::AdjacentFileList(const InternalKey& s, const InternalKey& l, const int level, std::vector<uint64_t>& fno_list){
    if(level == 100) return;

    std::cout << "fno_list size1 : " << fno_list.size() << std::endl;
    db_ptr_->AdjacentFileList(s, l, level, fno_list); //not yet check.

    std::cout << "fno list : " ;
    for(auto iter = fno_list.begin() ; iter != fno_list.end(); iter++){
    	std::cout << *iter << " " ;
    }
    std::cout << std::endl; 

    std::cout << "fno_list size2 : " << fno_list.size() << std::endl;
}

std::string time_in_HH_MM_SS_MMM()
{
    using namespace std::chrono;

    // get current time
    auto now = system_clock::now();

    // get number of milliseconds for the current second
    // (remainder after division into seconds)
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    // convert to std::time_t in order to convert to std::tm (broken time)
    auto timer = system_clock::to_time_t(now);

    // convert to broken time
    std::tm bt = *std::localtime(&timer);

    std::ostringstream oss;

    oss << std::put_time(&bt, "%H:%M:%S"); // HH:MM:SS
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();

    return oss.str();
}

IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;

  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  migrate_resource_.notify_one();
  return s;
}

IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint file_lifetime, uint32_t min_capacity, ZoneFile* zoneFile) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;
  IOStatus s;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;

  if (zone_alloc_mode == 1) {
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
    
  } else if (zone_alloc_mode == 2) {
    s = GetBestCAZAMatch(out_zone, zoneFile->smallest_, zoneFile->largest_, zoneFile->level_, min_capacity);
  }

  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }

  return s;
}

IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type, Zone **out_zone, ZoneFile* zoneFile) {
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;

{
  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }

  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) { // If finish_threashold > remaining_capacity 

    s = ApplyFinishThreshold(); 
    if (!s.ok()) {
      return s;
    }
  }

  WaitForOpenIOZoneToken(io_type == IOType::kWAL); // Waiting for new token (max open zones limit)
}

  /* 
     @ type 1 : default zenFS
     @ type 2 : CAZA
  */

  if (zone_alloc_mode == 1) { 
      /* Try to fill an already open zone(with the best life time diff) */
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
    if (!s.ok()) {
      PutOpenIOZoneToken(); //Giving it up 
      return s;
    }

    // Holding allocated_zone if != nullptr

    if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) { // level diff too big or equal?
      /* COULD_BE_WORSE = 50, if file_lifetime == zone_lifetime*/
      bool got_token = GetActiveIOZoneTokenIfAvailable();

      /* If we did not get a token, try to use the best match, even if the life
      * time diff not good but a better choice than to finish an existing zone
      * and open a new one
      */
      if (allocated_zone != nullptr) {
        if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
          Debug(logger_,
                "Allocator: avoided a finish by relaxing lifetime diff "
                "requirement\n");
        } else {
          s = allocated_zone->CheckRelease();
          if (!s.ok()) {
            PutOpenIOZoneToken();
            if (got_token) PutActiveIOZoneToken();
            return s;
          }
          allocated_zone = nullptr;
        }
      }

      /* If we haven't found an open zone to fill, open a new zone */
      if (allocated_zone == nullptr) {
        /* We have to make sure we can open an empty zone */
        while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
          s = FinishCheapestIOZone();
          if (!s.ok()) {
            PutOpenIOZoneToken();
            return s;
          }
        }

        s = AllocateEmptyZone(&allocated_zone);
        if (!s.ok()) {
          PutActiveIOZoneToken();
          PutOpenIOZoneToken();
          return s;
        }

        if (allocated_zone != nullptr) {
          assert(allocated_zone->IsBusy());
          allocated_zone->lifetime_ = file_lifetime;
          new_zone = true;
        } else {
          PutActiveIOZoneToken();
        }
      }
    } 
  } 
  else if (zone_alloc_mode == 2) { // mode = 2 (CAZA)
    std::cout << "zone_alloc_mode is 222222!!!!! file_lifetime : " << file_lifetime << \
      " smallest : " << zoneFile->smallest_.size() << " largest : " << zoneFile->largest_.size() << \
      " level : " << zoneFile->level_ << " ";
    std::cout << "zoneFile name : " << zoneFile->GetFilename() << std::endl;

    if(sst_to_zone_.empty()){
      // fprintf(stdout, "sst_to_zone is empty! ");
      if(GetActiveIOZoneTokenIfAvailable()){
        // fprintf(stdout, "Alloc Empty Zone --> ");
        s = AllocateEmptyZone(&allocated_zone);
        if (!s.ok()) {
          PutActiveIOZoneToken();
          PutOpenIOZoneToken();
          return s;
        }

        if (allocated_zone != nullptr) {
          assert(allocated_zone->IsBusy());
          allocated_zone->lifetime_ = file_lifetime;
          new_zone = true;
        } else {
          PutActiveIOZoneToken();
        }
      }
      if(allocated_zone != nullptr){
        fprintf(stdout, "\t\t\t emptyzone ");
      }
    } else{
      // fprintf(stdout, "Alloc GetBestCAZAMatch --> ");
      s = GetBestCAZAMatch(&allocated_zone, zoneFile->smallest_, zoneFile->largest_, zoneFile->level_);
      if(!s.ok()) {
        PutOpenIOZoneToken();
        return s;
      }
      if(allocated_zone != nullptr){
        fprintf(stdout, "\t\t\t caza ");
      }
    }

    if(allocated_zone != nullptr){
      fprintf(stdout, "----------->>>>>>> zone alloc id : %d\n", allocated_zone->zone_id_);
    }
    else {
      if(GetActiveIOZoneTokenIfAvailable()){
        // fprintf(stdout, "Get alloc Empty Zone!\n");
        s = AllocateEmptyZone(&allocated_zone);
        if (!s.ok()) {
          PutActiveIOZoneToken();
          PutOpenIOZoneToken();
          return s;
        }
        if (allocated_zone != nullptr) {
          assert(allocated_zone->IsBusy());
          allocated_zone->lifetime_ = file_lifetime;
          new_zone = true;
        } else {
          PutActiveIOZoneToken();
        }
        if(allocated_zone != nullptr){
          fprintf(stdout, "\t\t\t emptyzone ");
        }
      }
      else{
        // fprintf(stdout, "Try to fill an already open zone!\n");
        s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
        if (!s.ok()) {
          PutOpenIOZoneToken(); //Giving it up 
          return s;
        }
        if(allocated_zone != nullptr){
          fprintf(stdout, "\t\t\t alreadyOpen ");
        }
      }

      if(allocated_zone != nullptr){
        fprintf(stdout, "----------->>>>>>> zone alloc id : %d\n", allocated_zone->zone_id_);
      }
    }
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  *out_zone = allocated_zone;

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint32_t ZonedBlockDevice::GetBlockSize() { return zbd_be_->GetBlockSize(); }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint32_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(*zone);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
