//
// Created by daiyi on 2019/09/30.
//

#include <cassert>
#include "stats.h"
#include <cmath>
#include <iostream>
#include "plr.h"
#include "util.h"
#include <x86intrin.h>
#include "koo/koo.h"

using std::stoull;

namespace koo {

    Stats* Stats::singleton = nullptr;

#if BOURBON_PLUS
    Stats::Stats() : initial_time(__rdtsc()) {
    	  for(int i=0; i<20; i++)
    	  	timers.push_back(new Timer());
#else
    Stats::Stats() : timers(20, Timer{}), initial_time(__rdtsc()) {
#endif
        /*levelled_counters[0].name = "LevelModel";
        levelled_counters[1].name = "FileModel";
        levelled_counters[2].name = "Baseline";
        levelled_counters[3].name = "Succeeded";
        levelled_counters[4].name = "FalseInternal";
        levelled_counters[5].name = "Compaction";
        levelled_counters[6].name = "Learn";
        levelled_counters[7].name = "SuccessTime";
        levelled_counters[8].name = "FalseTime";
        levelled_counters[9].name = "FilteredLookup";
        levelled_counters[10].name = "PutWait";
        levelled_counters[11].name = "FileLearn";
        levelled_counters[12].name = "LevelLearn";
        levelled_counters[13].name = "LevelModelUse";
        levelled_counters[14].name = "LevelModelNotUse";*/
    }

    Stats* Stats::GetInstance() {
        if (!singleton) singleton = new Stats();
        return singleton;
    }

#if BOURBON_PLUS
    uint64_t Stats::StartTimer(uint32_t id) {
        Timer* timer = timers[id];
        return timer->Start();
    }

    std::pair<uint64_t, uint64_t> Stats::PauseTimer(uint64_t time_started, uint32_t id, bool record) {
        Timer* timer = timers[id];
        return timer->Pause(time_started, record);
    }

    void Stats::ResetTimer(uint32_t id) {
        Timer* timer = timers[id];
        timer->Reset();
    }

    uint64_t Stats::ReportTime(uint32_t id) {
        Timer* timer = timers[id];
        return timer->Time();
    }

    void Stats::ReportTime() {
        for (int i = 0; i < timers.size(); ++i) {
            printf("Timer %u: %lu\n", i, timers[i]->Time());
        }
    }
#else
    void Stats::StartTimer(uint32_t id) {
        Timer& timer = timers[id];
        timer.Start();
    }

    std::pair<uint64_t, uint64_t> Stats::PauseTimer(uint32_t id, bool record) {
        Timer& timer = timers[id];
        return timer.Pause(record);
    }

    void Stats::ResetTimer(uint32_t id) {
        Timer& timer = timers[id];
        timer.Reset();
    }

    uint64_t Stats::ReportTime(uint32_t id) {
        Timer& timer = timers[id];
        return timer.Time();
    }

    void Stats::ReportTime() {
        for (int i = 0; i < timers.size(); ++i) {
            printf("Timer %u: %lu\n", i, timers[i].Time());
        }
    }
#endif








    uint64_t Stats::GetTime() {
        unsigned int dummy = 0;
        uint64_t time_elapse = __rdtscp(&dummy) - initial_time;
        return time_elapse / reference_frequency;
    }


    void Stats::ResetAll() {
#if BOURBON_PLUS
        for (Timer* t: timers) t->Reset();
#else
        for (Timer& t: timers) t.Reset();
#endif
        initial_time = __rdtsc();
    }

    Stats::~Stats() {
#if BOURBON_PLUS
				for (Timer* t : timers) delete t;			//TODO
#else
        ReportTime();
#endif
    }

}
































