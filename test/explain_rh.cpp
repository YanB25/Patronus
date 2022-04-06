#include <algorithm>
#include <chrono>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "patronus/All.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr uint32_t kMachineNr = 2;

using namespace patronus::hash;
using namespace patronus;

template <size_t kE, size_t kB, size_t kS>
void explain(Patronus::pointer patronus)
{
    using RaceHashingT = RaceHashing<kE, kB, kS>;

    auto rh_allocator = patronus->get_allocator(Patronus::kDefaultHint);

    RaceHashingConfig conf;
    conf.initial_subtable = 1;
    conf.g_kvblock_pool_size = 4_KB;
    conf.g_kvblock_pool_addr = rh_allocator->alloc(4_KB);
    auto server_rdma_ctx = patronus::RdmaAdaptor::new_instance(patronus);

    // borrow from the master's kAllocHintDirSubtable allocator

    auto rh = RaceHashingT::new_instance(server_rdma_ctx, rh_allocator, conf);

    auto max_capacity = rh->max_capacity();
    auto search_nr = (kS - 1) * 4;
    LOG(ERROR) << "RH<" << kE << ", " << kB << ", " << kS << "> "
               << pre_rh_explain(*rh)
               << " If kvblock 64B: " << 64 * max_capacity
               << " B. If 4KB: " << 4_KB * max_capacity
               << " B. Each time search " << search_nr
               << " slots, prob (/2**8): " << 1.0 * search_nr / (1 << 8);

    rh_allocator->free(conf.g_kvblock_pool_addr, conf.g_kvblock_pool_size);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.block_class = {16_MB};
    pconfig.block_ratio = {1};
    pconfig.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(pconfig);
    patronus->registerServerThread();

    explain<32, 16, 8>(patronus);
    explain<32, 32, 8>(patronus);  // use this
    explain<32, 4096, 16>(patronus);
    explain<32, 8192, 16>(patronus);  // use this

    LOG(INFO) << "finished. ctrl+C to quit.";
}