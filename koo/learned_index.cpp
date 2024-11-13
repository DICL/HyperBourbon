//
// Created by daiyi on 2020/02/02.
//

#include "koo/learned_index.h"

#include "db/version_set.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <utility>
#include "util/mutexlock.h"
#include "koo/util.h"
#include "koo/koo.h"

namespace koo {

std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(
    const Slice& target_x) const {
  assert(string_segments.size() > 1);
  ++served;

  // check if the key is within the model bounds
  uint64_t target_int = SliceToInteger(target_x);
  if (target_int > max_key) return std::make_pair(size, size);
  if (target_int < min_key) return std::make_pair(size, size);

  // binary search between segments
  uint32_t left = 0, right = (uint32_t)string_segments.size() - 1;
  while (left != right - 1) {
    uint32_t mid = (right + left) / 2;
    if (target_int < string_segments[mid].x)
      right = mid;
    else
      left = mid;
  }

	// TODO x보다 1 작은 키 찾는 버그
  /*if (left < (uint32_t)string_segments.size()-1 && target_int+1 == string_segments[left+1].x)
  	left++;*/

  // calculate the interval according to the selected segment
  double result =
      target_int * string_segments[left].k + string_segments[left].b;
  result = is_level ? result / 2 : result;
  uint64_t lower =
      result - error > 0 ? (uint64_t)std::floor(result - error) : 0;
  uint64_t upper = (uint64_t)std::ceil(result + error);
  if (lower >= size) return std::make_pair(size, size);
  upper = upper < size ? upper : size - 1;
  //                printf("%s %s %s\n", string_keys[lower].c_str(),
  //                string(target_x.data(), target_x.size()).c_str(),
  //                string_keys[upper].c_str()); assert(target_x >=
  //                string_keys[lower] && target_x <= string_keys[upper]);

  return std::make_pair(lower, upper);
}

uint64_t LearnedIndexData::MaxPosition() const { return size - 1; }

double LearnedIndexData::GetError() const { return error; }

// Actual function doing learning
bool LearnedIndexData::Learn() {
  // FILL IN GAMMA (error)
  PLR plr = PLR(LEARN_MODEL_ERROR);

  // check if data if filled
  if (string_keys.empty()) assert(false);

  // fill in some bounds for the model
  uint64_t temp = atoll(string_keys.back().c_str());
  min_key = atoll(string_keys.front().c_str());
  max_key = atoll(string_keys.back().c_str());
  size = string_keys.size();

  // actual training
  std::vector<Segment> segs = plr.train(string_keys, !is_level);
  if (segs.empty()) return false;
  // fill in a dummy last segment (used in segment binary search)
  segs.push_back((Segment){temp, 0, 0});
  string_segments = std::move(segs);

  learned.store(true);
  return true;
}

// static learning function to be used with LevelDB background scheduling
// level learning
void LearnedIndexData::LevelLearn(void* arg, bool nolock) {
  /*Stats* instance = Stats::GetInstance();
  bool success = false;
  bool entered = false;
  instance->StartTimer(8);

  VersionAndSelf* vas = reinterpret_cast<VersionAndSelf*>(arg);
  LearnedIndexData* self = vas->self;
  self->is_level = true;
  self->level = vas->level;
  Version* c;
  if (!nolock) {
    c = db->GetCurrentVersion();
  }
  if (db->version_count == vas->v_count) {
    entered = true;
    if (vas->version->FillLevel(koo::read_options, vas->level)) {
      self->filled = true;
      if (db->version_count == vas->v_count) {
        if (env->compaction_awaiting.load() == 0 && self->Learn()) {
          success = true;
        } else {
          self->learning.store(false);
        }
      }
    }
  }
  if (!nolock) {
    koo::db->ReturnCurrentVersion(c);
  }

  auto time = instance->PauseTimer(8, true);

  if (entered) {
    self->cost = time.second - time.first;
    learn_counter_mutex.Lock();
    events[1].push_back(new LearnEvent(time, 0, self->level, success));
    levelled_counters[6].Increment(vas->level, time.second - time.first);
    learn_counter_mutex.Unlock();
  }

  delete vas;*/
}

// static learning function to be used with LevelDB background scheduling
// file learning
uint64_t LearnedIndexData::FileLearn(void* arg) {
  Stats* instance = Stats::GetInstance();
  bool entered = false;
  instance->StartTimer(11);

  MetaAndSelf* mas = reinterpret_cast<MetaAndSelf*>(arg);
  LearnedIndexData* self = mas->self;
  self->learning.store(true);
  self->level = mas->level;

  Version* c = db->GetCurrentVersion();
  if (self->FillData(c, mas->meta)) {
    self->Learn();
    entered = true;
  }
  self->learning.store(false);
  koo::db->ReturnCurrentVersion(c);

  auto time = instance->PauseTimer(11, true);
  if (entered) {
    // count how many file learning are done.
    self->cost = time.second - time.first;
  }

  //        if (fresh_write) {
  //            self->WriteModel(koo::db->versions_->dbname_ + "/" +
  //            to_string(mas->meta->number) + ".fmodel");
  //            self->string_keys.clear();
  //            self->num_entries_accumulated.array.clear();
  //        }
#if BOURBON_PLUS
	self->string_keys.clear();
	self->string_keys.shrink_to_fit();
	if (self->Deleted()) {
		self->string_segments.clear();
		self->string_segments.shrink_to_fit();
	}
#endif
  if (!fresh_write) delete mas->meta;
  delete mas;
  return entered ? time.second - time.first : 0;
}

// general model checker
bool LearnedIndexData::Learned() {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  } else
    return false;
}

// level model checker, used to be also learning trigger
bool LearnedIndexData::Learned(Version* version, int v_count, int level) {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  }
  return false;
  //        } else {
  //            if (level_learning_enabled && ++current_seek >= allowed_seek &&
  //            !learning.exchange(true)) {
  //                env->ScheduleLearning(&LearnedIndexData::Learn, new
  //                VersionAndSelf{version, v_count, this, level}, 0);
  //            }
  //            return false;
  //        }
}

// file model checker, used to be also learning trigger
bool LearnedIndexData::Learned(Version* version, int v_count,
                               FileMetaData* meta, int level) {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  } else
    return false;
  //        } else {
  //            if (file_learning_enabled && (true || level != 0 && level != 1)
  //            && ++current_seek >= allowed_seek && !learning.exchange(true)) {
  //                env->ScheduleLearning(&LearnedIndexData::FileLearn, new
  //                MetaAndSelf{version, v_count, meta, this, level}, 0);
  //            }
  //            return false;
  //        }
}

bool LearnedIndexData::FillData(Version* version, FileMetaData* meta) {
  // if (filled) return true;

  if (version->FillData(koo::read_options, meta, this)) {
    // filled = true;
    return true;
  }
  return false;
}

void LearnedIndexData::WriteModel(const string& filename) {
  if (!learned.load()) return;
	std::ofstream ofs(filename, std::ios::binary);
	ofs.write(reinterpret_cast<const char*>(&koo::block_num_entries), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&koo::block_size), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&koo::entry_size), sizeof(uint64_t));
	size_t segs_size = string_segments.size();
	ofs.write(reinterpret_cast<const char*>(&segs_size), sizeof(size_t));
	for (Segment& s : string_segments) {
		ofs.write(reinterpret_cast<const char*>(&s.x), sizeof(uint64_t));
		ofs.write(reinterpret_cast<const char*>(&s.k), sizeof(double));
		ofs.write(reinterpret_cast<const char*>(&s.b), sizeof(double));
	}
	ofs.write(reinterpret_cast<const char*>(&min_key), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&max_key), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&size), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&level), sizeof(int));
	ofs.write(reinterpret_cast<const char*>(&cost), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&file_number), sizeof(uint64_t));
	ofs.close();
}

void LearnedIndexData::ReadModel(const string& filename) {
	if (learned.load()) return;

	std::ifstream ifs(filename, std::ios::binary);
	if (!ifs.good()) return;
	ifs.read(reinterpret_cast<char*>(&koo::block_num_entries), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&koo::block_size), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&koo::entry_size), sizeof(uint64_t));
	size_t segs_size;
	ifs.read(reinterpret_cast<char*>(&segs_size), sizeof(size_t));
	for (int i=0; i<segs_size; i++) {
		uint64_t x, x_last;
		double k, b;
		uint32_t y_last;
		ifs.read(reinterpret_cast<char*>(&x), sizeof(uint64_t));
		ifs.read(reinterpret_cast<char*>(&k), sizeof(double));
		ifs.read(reinterpret_cast<char*>(&b), sizeof(double));
		string_segments.emplace_back(Segment(x, k, b));
	}
	ifs.read(reinterpret_cast<char*>(&min_key), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&max_key), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&level), sizeof(int));
	ifs.read(reinterpret_cast<char*>(&cost), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&file_number), sizeof(uint64_t));
	ifs.close();

  learned.store(true);
}

#if BOURBON_PLUS
LearnedIndexData::~LearnedIndexData() {
	//if (!buckets_data) delete buckets_data;
	//buckets_data = nullptr;
	// TODO unlink write했던 파일들 삭제
}

bool LearnedIndexData::Deleted() {
  if (deleted_not_atomic) return true;
  else if (deleted.load()) {
    deleted_not_atomic = true;
    return true;
  } else return false;
}

void LearnedIndexData::MarkDelete() {
	deleted.store(true);

	mutex_delete_.Lock();
	if (!learning.load()) {
		string_segments.clear();
		string_segments.shrink_to_fit();
	}
	mutex_delete_.Unlock();
}

void FileLearnedIndexData::DeleteModel(int number) {
	//leveldb::MutexLock l(&mutex);
	if (file_learned_index_data.size() <= number) return;
	if (file_learned_index_data[number] == nullptr) return;

	file_learned_index_data[number]->MarkDelete();
	//delete file_learned_index_data[number];
	//file_learned_index_data[number] = nullptr;
	return;
}
#endif

void LearnedIndexData::ReportStats() {
  //        double neg_gain, pos_gain;
  //        if (num_neg_model == 0 || num_neg_baseline == 0) {
  //            neg_gain = 0;
  //        } else {
  //            neg_gain = ((double) time_neg_baseline / num_neg_baseline -
  //            (double) time_neg_model / num_neg_model) * num_neg_model;
  //        }
  //        if (num_pos_model == 0 || num_pos_baseline == 0) {
  //            pos_gain = 0;
  //        } else {
  //            pos_gain = ((double) time_pos_baseline / num_pos_baseline -
  //            (double) time_pos_model / num_pos_model) * num_pos_model;
  //        }

  printf("%d %d %lu %lu %lu\n", level, served, string_segments.size(), cost,
         size);  //, file_size);
  //        printf("\tPredicted: %lu %lu %lu %lu %d %d %d %d %d %lf\n",
  //        time_neg_baseline_p, time_neg_model_p, time_pos_baseline_p,
  //        time_pos_model_p,
  //                num_neg_baseline_p, num_neg_model_p, num_pos_baseline_p,
  //                num_pos_model_p, num_files_p, gain_p);
  //        printf("\tActual: %lu %lu %lu %lu %d %d %d %d %f\n",
  //        time_neg_baseline, time_neg_model, time_pos_baseline,
  //        time_pos_model,
  //               num_neg_baseline, num_neg_model, num_pos_baseline,
  //               num_pos_model, pos_gain + neg_gain);
}

void LearnedIndexData::FillCBAStat(bool positive, bool model, uint64_t time) {
  //        int& num_to_update = positive ? (model ? num_pos_model :
  //        num_pos_baseline) : (model ? num_neg_model : num_neg_baseline);
  //        uint64_t& time_to_update =  positive ? (model ? time_pos_model :
  //        time_pos_baseline) : (model ? time_neg_model : time_neg_baseline);
  //        time_to_update += time;
  //        num_to_update += 1;
}

LearnedIndexData* FileLearnedIndexData::GetModel(int number) {
#if BOURBON_PLUS
  if (file_learned_index_data.size() <= number) {
  	mutex.Lock();
		if (file_learned_index_data.size() <= number) {
			file_learned_index_data.resize(number + 100, nullptr);
			file_learned_index_data[number] = new LearnedIndexData(file_allowed_seek, false, (uint64_t)number);
			mutex.Unlock();
			return file_learned_index_data[number];
		}
		mutex.Unlock();
	}
  if (file_learned_index_data[number] == nullptr) {
  	mutex.Lock();
		if (file_learned_index_data[number] == nullptr) {
			file_learned_index_data[number] = new LearnedIndexData(file_allowed_seek, false, number);
			mutex.Unlock();
			return file_learned_index_data[number];
		}
		mutex.Unlock();
	}
	return file_learned_index_data[number];
#else
  leveldb::MutexLock l(&mutex);
  if (file_learned_index_data.size() <= number)
    file_learned_index_data.resize(number + 1, nullptr);
  if (file_learned_index_data[number] == nullptr)
    file_learned_index_data[number] = new LearnedIndexData(file_allowed_seek, false, number);
  return file_learned_index_data[number];
#endif
}

#if BOURBON_PLUS
LearnedIndexData* FileLearnedIndexData::GetModelForLookup(int number) {
  if (file_learned_index_data.size() <= number) return nullptr;
  if (file_learned_index_data[number] == nullptr) return nullptr;
  return file_learned_index_data[number];
}
#endif

bool FileLearnedIndexData::FillData(Version* version, FileMetaData* meta) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->FillData(version, meta);
}

std::vector<std::string>& FileLearnedIndexData::GetData(FileMetaData* meta) {
  auto* model = GetModel(meta->number);
  return model->string_keys;
}

bool FileLearnedIndexData::Learned(Version* version, FileMetaData* meta,
                                   int level) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->Learned(version, db->version_count, meta, level);
}

AccumulatedNumEntriesArray* FileLearnedIndexData::GetAccumulatedArray(
    int file_num) {
  auto* model = GetModel(file_num);
  return &model->num_entries_accumulated;
}

std::pair<uint64_t, uint64_t> FileLearnedIndexData::GetPosition(
    const Slice& key, int file_num) {
  return file_learned_index_data[file_num]->GetPosition(key);
}

FileLearnedIndexData::~FileLearnedIndexData() {
  leveldb::MutexLock l(&mutex);
  for (auto pointer : file_learned_index_data) {
		if (pointer != nullptr) delete pointer;
  }
}

void FileLearnedIndexData::Report() {
  leveldb::MutexLock l(&mutex);

  std::set<uint64_t> live_files;
  //koo::db->versions_->AddLiveFiles(&live_files);

  for (size_t i = 0; i < file_learned_index_data.size(); ++i) {
    auto pointer = file_learned_index_data[i];
    if (pointer != nullptr && pointer->cost != 0) {
      printf("FileModel %lu %d ", i, i > watermark);
      pointer->ReportStats();
    }
  }
}

void AccumulatedNumEntriesArray::Add(uint64_t num_entries, string&& key) {
  array.emplace_back(num_entries, key);
}

bool AccumulatedNumEntriesArray::Search(const Slice& key, uint64_t lower,
                                        uint64_t upper, size_t* index,
                                        uint64_t* relative_lower,
                                        uint64_t* relative_upper) {
  if (koo::MOD == 4) {
    uint64_t lower_pos = lower / array[0].first;
    uint64_t upper_pos = upper / array[0].first;
    if (lower_pos != upper_pos) {
      while (true) {
        if (lower_pos >= array.size()) return false;
        //if (key <= array[lower_pos].second) break;
        lower = array[lower_pos].first;
        ++lower_pos;
      }
      upper = std::min(upper, array[lower_pos].first - 1);
      *index = lower_pos;
      *relative_lower =
          lower_pos > 0 ? lower - array[lower_pos - 1].first : lower;
      *relative_upper =
          lower_pos > 0 ? upper - array[lower_pos - 1].first : upper;
      return true;
    }
    *index = lower_pos;
    *relative_lower = lower % array[0].first;
    *relative_upper = upper % array[0].first;
    return true;

  } else {
    size_t left = 0, right = array.size() - 1;
    while (left < right) {
      size_t mid = (left + right) / 2;
      if (lower < array[mid].first)
        right = mid;
      else
        left = mid + 1;
    }

    if (upper >= array[left].first) {
      while (true) {
        if (left >= array.size()) return false;
        //if (key <= array[left].second) break;
        lower = array[left].first;
        ++left;
      }
      upper = std::min(upper, array[left].first - 1);
    }

    *index = left;
    *relative_lower = left > 0 ? lower - array[left - 1].first : lower;
    *relative_upper = left > 0 ? upper - array[left - 1].first : upper;
    return true;
  }
}

bool AccumulatedNumEntriesArray::SearchNoError(uint64_t position, size_t* index,
                                               uint64_t* relative_position) {
  *index = position / array[0].first;
  *relative_position = position % array[0].first;
  return *index < array.size();

  //        size_t left = 0, right = array.size() - 1;
  //        while (left < right) {
  //            size_t mid = (left + right) / 2;
  //            if (position < array[mid].first) right = mid;
  //            else left = mid + 1;
  //        }
  //        *index = left;
  //        *relative_position = left > 0 ? position - array[left - 1].first :
  //        position; return left < array.size();
}

uint64_t AccumulatedNumEntriesArray::NumEntries() const {
  return array.empty() ? 0 : array.back().first;
}

}  // namespace koo
