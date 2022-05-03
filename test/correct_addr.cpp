#include <algorithm>
#include <random>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;

    auto patronus = Patronus::ins(config);
    auto dsm = patronus->get_dsm();

    size_t buf_offset = 1024;
    size_t buf_dsm_offset = dsm->buffer_offset_to_dsm_offset(buf_offset);
    auto *buf_addr = dsm->buffer_offset_to_addr(buf_offset);
    auto *buf_addr2 = dsm->dsm_offset_to_addr(buf_dsm_offset);
    CHECK_EQ(buf_addr, buf_addr2);
    CHECK_EQ(dsm->addr_to_dsm_offset(buf_addr), buf_dsm_offset);
    CHECK_EQ(
        buf_offset,
        dsm->addr_to_buffer_offset(dsm->buffer_offset_to_addr(buf_offset)));

    size_t dsm_offset = 777;
    CHECK_EQ(dsm_offset,
             dsm->addr_to_dsm_offset(dsm->dsm_offset_to_addr(dsm_offset)));

    size_t another_buf_offset = 23463;
    CHECK_EQ(another_buf_offset,
             dsm->dsm_offset_to_buffer_offset(
                 dsm->buffer_offset_to_dsm_offset(another_buf_offset)));

    LOG(INFO) << "finished. ctrl+C to quit.";
}