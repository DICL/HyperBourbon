// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/table.h"
#include "util/coding.h"
#include "util/coding.h"
#include "table/filter_block.h"
#include "table/block.h"
#include "koo/stats.h"
#include "koo/koo.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& /*key*/, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = LDBTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&), int level,
                       FileMetaData* meta, uint64_t lower, uint64_t upper,
                       bool learned, Version* version,
                       koo::LearnedIndexData** model, bool* file_learned) {
  Cache::Handle* handle = NULL;
  koo::Stats* instance = koo::Stats::GetInstance();

#if BOURBON_PLUS
	koo::LearnedIndexData* model_ = koo::file_data->GetModelForLookup(meta->number);
	if (model_ != nullptr) {
		*model = model_;
#else
  	*model = koo::file_data->GetModel(meta->number);
  	assert(file_learned != nullptr);
#endif
  	*file_learned = (*model)->Learned();

  	if (learned || *file_learned) {
  		LevelRead(options, file_number, file_size, k, arg, handle_result, level,
								meta, lower, upper, learned, version);
			return Status::OK();
		}
#if BOURBON_PLUS
	}
#endif

  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result, level, meta, lower, upper, learned, version);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

void TableCache::LevelRead(const ReadOptions& options, uint64_t file_number, 
														uint64_t file_size, const Slice& k, void* arg, 
														void (*handle_result)(void*, const Slice&, const Slice&), int level,
														FileMetaData* meta, uint64_t lower, uint64_t upper,
														bool learned, Version* version) {
	koo::Stats* instance = koo::Stats::GetInstance();

	// Find table
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(handle));
  RandomAccessFile* file = tf->file;
  FilterBlockReader* filter = tf->table->rep_->filter;

	if (!learned) {
		ParsedInternalKey parsed_key;
	  ParseInternalKey(k, &parsed_key);
	  koo::LearnedIndexData* model = koo::file_data->GetModel(meta->number);
		auto bounds = model->GetPosition(parsed_key.user_key);
		lower = bounds.first;
	  upper = bounds.second;
		if (lower > model->MaxPosition()) {
			cache_->Release(handle);
			return;
		}
	}

  // Get the position we want to read
  // Get the data block index
  size_t index_lower = lower / koo::block_num_entries;
  size_t index_upper = upper / koo::block_num_entries;

  // if the given interval overlaps two data block, consult the index block to get
  // the largest key in the first data block and compare it with the target key
  // to decide which data block the key is in
  uint64_t i = index_lower;
  if (index_lower != index_upper) {
    Block* index_block = tf->table->rep_->index_block;
    uint32_t mid_index_entry = DecodeFixed32(index_block->data_ + index_block->restart_offset_ + index_lower * sizeof(uint32_t));
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(index_block->data_ + mid_index_entry,
                                      index_block->data_ + index_block->restart_offset_, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
    i = comp < 0 ? index_upper : index_lower;
  }

  // Check Filter Block
  uint64_t block_offset = i * koo::block_size;
  if (filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {
    cache_->Release(handle);
    return;
  }

  // Get the interval within the data block that the target key may lie in
  size_t pos_block_lower = i == index_lower ? lower % koo::block_num_entries : 0;
  size_t pos_block_upper = i == index_upper ? upper % koo::block_num_entries : koo::block_num_entries - 1;

  // Read corresponding entries
  size_t read_size = (pos_block_upper - pos_block_lower + 1) * koo::entry_size;
  static char scratch[4096];
  Slice entries;
  s = file->Read(block_offset + pos_block_lower * koo::entry_size, read_size, &entries, scratch);
  assert(s.ok());

  // Binary Search within the interval
  uint64_t left = pos_block_lower, right = pos_block_upper;
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(entries.data() + (mid - pos_block_lower) * koo::entry_size,
            entries.data() + read_size, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
    if (comp < 0) left = mid + 1;
    else right = mid;
  }

  // decode the target entry to get the key and value (actually value_addr)
  uint32_t shared, non_shared, value_length;
  const char* key_ptr = DecodeEntry(entries.data() + (left - pos_block_lower) * koo::entry_size,
          entries.data() + read_size, &shared, &non_shared, &value_length);
  assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
  Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
  handle_result(arg, key, value);

	cache_->Release(handle);
}

bool TableCache::FillData(const ReadOptions& options, FileMetaData* meta, koo::LearnedIndexData* data) {
	Cache::Handle* handle = nullptr;
	Status s = FindTable(meta->number, meta->file_size, &handle);

	if (s.ok()) {
		Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
		table->FillData(options, data);
		cache_->Release(handle);
		return true;
	} else return false;
}

}  // namespace leveldb
