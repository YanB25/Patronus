#pragma once

#ifndef SHERMEM_STACKTRACE_H_
#define SHERMEM_STACKTRACE_H_

#define UNW_LOCAL_ONLY
#include <glog/logging.h>

#include <iostream>

#include "libunwind.h"

namespace util
{
struct StackTrace
{
};
static StackTrace stack_trace;

inline std::ostream &operator<<(std::ostream &os, const StackTrace &)
{
    unw_cursor_t cursor;
    unw_context_t uc;
    unw_word_t ip, sp;
    char buf[4096];
    unw_word_t offset;
    unw_getcontext(&uc);           // store registers
    unw_init_local(&cursor, &uc);  // initialze with context

    os << "\n";

    while (unw_step(&cursor) > 0)
    {                                           // unwind to older stack frame
        unw_get_reg(&cursor, UNW_REG_IP, &ip);  // read register, rip
        unw_get_reg(&cursor, UNW_REG_SP, &sp);  // read register, rbp
        unw_get_proc_name(&cursor, buf, 4095, &offset);  // get name and offset
        os << "0x" << std::hex << ip << " <" << buf << "+0x" << std::hex
           << offset << ">\n";
    }
    os << std::endl;
    return os;
}

}  // namespace util

#endif