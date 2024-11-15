// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "hyperleveldb/options.h"

#include "hyperleveldb/comparator.h"
#include "hyperleveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(64<<20),
      max_open_files(1024 * 1024),
      //max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kNoCompression),
      //compression(kSnappyCompression),
      filter_policy(NULL),
      manual_garbage_collection(false) {
}


}  // namespace leveldb
