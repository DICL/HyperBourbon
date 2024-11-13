#pragma once

#include <iostream>
#include <chrono>
#include <math.h>
#include <fstream>
#include <atomic>
#include <cstdio>
#include <unordered_set>
#include <unordered_map>
#include <thread>
#include <unistd.h>
#include <sys/syscall.h>
#include <experimental/filesystem>

#define LEARN_MODEL_ERROR 8
#define BOURBON_PLUS 1				// model, cba mutex concurrency 문제 줄이고 model 삭제해주는거. vlog, stats, timer consistency 문제 해결


namespace koo {

#if BOURBON_PLUS
class SpinLock {
 public:
  SpinLock() : flag_(false){}
  void lock() {
  	bool expect = false;
  	while (!flag_.compare_exchange_weak(expect, true)){
  		expect = false;
		}
	}
	void unlock(){
		flag_.store(false);
	}

 private:
  std::atomic<bool> flag_;
};
#endif

}
