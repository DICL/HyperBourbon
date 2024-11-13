//
// Created by daiyi on 2020/02/02.
//

#include "timer.h"
#include "util.h"
#include <cassert>
#include <x86intrin.h>
#include "koo/koo.h"


namespace koo {

#if BOURBON_PLUS
    Timer::Timer() : time_accumulated(0) {}
#else
    Timer::Timer() : time_accumulated(0), started(false) {}
#endif

#if BOURBON_PLUS
    uint64_t Timer::Start() {
        unsigned int dummy = 0;
        return __rdtscp(&dummy);
    }
#else
    void Timer::Start() {
        assert(!started);
        unsigned int dummy = 0;
        time_started = __rdtscp(&dummy);
        started = true;
    }
#endif

#if BOURBON_PLUS
    std::pair<uint64_t, uint64_t> Timer::Pause(uint64_t time_started, bool record) {
#else
    std::pair<uint64_t, uint64_t> Timer::Pause(bool record) {
        assert(started);
#endif
        unsigned int dummy = 0;
        uint64_t time_elapse = __rdtscp(&dummy) - time_started;
        time_accumulated += time_elapse / reference_frequency;

        if (record) {
            Stats* instance = Stats::GetInstance();
            uint64_t start_absolute = time_started - instance->initial_time;
            uint64_t end_absolute = start_absolute + time_elapse;
#if !BOURBON_PLUS
            started = false;
#endif
            return {start_absolute / reference_frequency, end_absolute / reference_frequency};
        } else {
#if !BOURBON_PLUS
            started = false;
#endif
            return {0, 0};
        }
    }

    void Timer::Reset() {
        time_accumulated = 0;
#if !BOURBON_PLUS
        started = false;
#endif
    }

    uint64_t Timer::Time() {
        //assert(!started);
        return time_accumulated;
    }
}
