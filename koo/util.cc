#include "koo/util.h"
#include "util/mutexlock.h"
#include <x86intrin.h>
#include "koo/learned_index.h"

namespace koo {


	int MOD = 7;
	uint32_t level_model_error = 1;

	FileLearnedIndexData* file_data = nullptr;
	CBModel_Learn* learn_cb_model = nullptr;
	leveldb::Env* env;
	leveldb::DBImpl* db;
	leveldb::ReadOptions read_options;
	leveldb::WriteOptions write_options;
	uint64_t initial_time = 0;

	//uint64_t fd_limit = 1024 * 1024;
	bool fresh_write = false;			// TODO ???
	bool block_num_entries_recorded = false;
	uint64_t block_num_entries = 0;
	uint64_t block_size = 0;
	uint64_t entry_size = 0;
	float reference_frequency = 2.6;
	uint64_t learn_trigger_time = 50000000;
	int policy = 0;
	int level_allowed_seek = 1;
	int file_allowed_seek = 10;

	leveldb::port::Mutex file_stats_mutex;
	map<int, FileStats> file_stats;

  uint64_t SliceToInteger(const Slice& slice) {
    const char* data = slice.data();
    size_t size = slice.size();
    uint64_t num = 0;
		bool leading_zeros = true;

		for (int i = 0; i < size; ++i) {
			int temp = data[i];
      if (leading_zeros && temp == '0') continue;
      leading_zeros = false;
      num = (num << 3) + (num << 1) + temp - 48;
    }
    return num;
  }


}
