#include <glog/logging.h>

#include <algorithm>
#include <random>

#include "DSM.h"
#include "PerThread.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Type.h"
#include "umsg/Config.h"
#include "util/Rand.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");
constexpr static size_t kBurnCnt = 100_M;
// constexpr static size_t kBurnCnt = 1_M;
constexpr static size_t kClientThreadNr = kMaxAppThread - 1;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

struct ReqMsg
{
    uint16_t tid;
    uint16_t nid;
    char buf[sizeof(patronus::AcquireRequest) - 2 * sizeof(uint16_t)];
} __attribute__((packed));

struct RespMsg
{
    uint16_t tid;
    uint16_t nid;
    char buf[sizeof(patronus::AcquireResponse) - 2 * sizeof(uint16_t)];
} __attribute__((packed));

// constexpr static size_t kAllowedInflight =
//     config::umsg::kExpectInFlightMessageNr;
// constexpr static size_t kTokenNr = 1;
// constexpr static ssize_t kMessageEachToken = kAllowedInflight / kTokenNr;

constexpr static size_t kAllowedInflight = 64;
constexpr static size_t kTokenNr = 64;
constexpr static ssize_t kMessageEachToken = kAllowedInflight / kTokenNr;

static_assert(kAllowedInflight <= config::umsg::kExpectInFlightMessageNr);

void client_burn(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;

    auto server_nid = ::config::get_server_nids().front();

    Timer t;
    t.begin();

    Perthread<std::atomic<size_t>> counts;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, &count = counts[i], server_nid]() {
            dsm->registerThread();
            auto tid = dsm->get_thread_id();
            auto nid = dsm->get_node_id();
            auto to_dir_id = tid % NR_DIRECTORY;

            auto rdma_buffer = dsm->get_rdma_buffer();
            auto *buf = rdma_buffer.buffer;
            DCHECK_LT(sizeof(ReqMsg), rdma_buffer.size);
            auto *send_msg = (ReqMsg *) buf;
            send_msg->tid = tid;
            send_msg->nid = nid;

            size_t sent = 0;
            char buffer[config::umsg::kUserMessageSize *
                        config::umsg::kRecvLimit];
            int64_t token = kTokenNr;
            for (size_t t = 0; t < kBurnCnt; ++t)
            {
                dsm->unreliable_send(
                    buf, sizeof(ReqMsg), server_nid, to_dir_id);
                sent++;
                if (sent % kMessageEachToken == 0)
                {
                    count.fetch_add(kMessageEachToken,
                                    std::memory_order_relaxed);

                    token--;
                    VLOG(1) << "[wait] tid " << tid << " sent " << sent
                            << " at " << kMessageEachToken << ", wait for ack.";
                    do
                    {
                        size_t recv_nr = dsm->unreliable_try_recv(
                            buffer, config::umsg::kRecvLimit);
                        // handle possbile recv token
                        for (size_t r = 0; r < recv_nr; ++r)
                        {
                            void *msg_addr =
                                buffer + config::umsg::kUserMessageSize * r;
                            auto *recv_msg = (RespMsg *) msg_addr;
                            std::ignore = recv_msg;
                            VLOG(3) << "[wait] tid " << tid
                                    << " recv continue msg from TODO. add one "
                                       "token";
                            token++;
                        }
                        if (token <= 0)
                        {
                            // VLOG(1) << "[wait] tid " << tid
                            //         << " blocked. finished " << t;
                        }
                    } while (token <= 0);
                    VLOG(1) << "[wait] tid " << tid
                            << " has enough token. current: " << token;
                }
            }
            LOG(INFO) << "[debug] client tid " << tid << " sent " << sent;
        });
    }

    size_t expect_nr = thread_nr * kBurnCnt;

    auto start = std::chrono::steady_clock::now();
    size_t last_sum = 0;
    while (true)
    {
        size_t sum = 0;
        for (size_t i = 0; i < thread_nr; ++i)
        {
            sum += counts[i].load(std::memory_order_relaxed);
        }
        auto end = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        double ops = 1.0 * 1e9 * (sum - last_sum) / ns;
        double guess_cluster_ops = ops * ::config::get_client_nids().size();
        LOG(INFO) << "[bench] OPS: " << ops << " (guess cluster "
                  << guess_cluster_ops << ")"
                  << ", for sum: " << (sum - last_sum) << ", ns " << ns;

        start = end;
        last_sum = sum;

        std::this_thread::sleep_for(1s);
        if (sum == expect_nr)
        {
            break;
        }
    }

    for (auto &t : threads)
    {
        t.join();
    }

    auto ns = t.end();
    auto op = thread_nr * kBurnCnt;
    double ops = 1e9 * op / ns;
    LOG(INFO) << "count: " << op << ", ns: " << ns << ", ops: " << ops
              << ", with thread " << thread_nr
              << ". ops/thread: " << (ops / thread_nr);
}

void server_burn_do(
    DSM::pointer dsm,
    std::atomic<bool> &finished,
    std::array<Perthread<std::atomic<size_t>>, MAX_MACHINE> &gots)
{
    std::ignore = gots;
    auto tid = dsm->get_thread_id();

    char buffer[config::umsg::kUserMessageSize * config::umsg::kRecvLimit];
    size_t work_nr{0};
    LOG(INFO) << "[bench] server tid " << tid << " started.";

    auto rdma_buf = dsm->get_rdma_buffer();
    auto rdma_buf_nr = rdma_buf.size / sizeof(RespMsg);
    size_t rdma_buf_id = 0;
    memset(rdma_buf.buffer, 0, sizeof(RespMsg));

    while (!finished.load(std::memory_order_relaxed))
    {
        auto get = dsm->unreliable_try_recv(buffer, config::umsg::kRecvLimit);
        if (get > 0)
        {
            for (size_t m = 0; m < get; ++m)
            {
                auto *resp_rdma_buf =
                    (rdma_buf.buffer + sizeof(RespMsg) * rdma_buf_id);
                rdma_buf_id = (rdma_buf_id + 1) % rdma_buf_nr;

                work_nr++;
                auto &msg =
                    *(ReqMsg *) (buffer + config::umsg::kUserMessageSize * m);
                auto from_tid = msg.tid;
                DCHECK_LT(from_tid, kMaxAppThread);
                auto from_nid = msg.nid;
                DCHECK_LT(from_nid, MAX_MACHINE);
                // VLOG(1) << "[debug] server tid " << tid
                //         << " got message client: tid: " << from_tid
                //         << ", nid: " << from_nid << " at " << work_nr <<
                //         "-th";

                auto *resp_msg = (RespMsg *) resp_rdma_buf;
                resp_msg->tid = from_tid;
                resp_msg->nid = from_nid;
                bool succ = dsm->unreliable_prepare_send((char *) resp_rdma_buf,
                                                         sizeof(RespMsg),
                                                         from_nid,
                                                         from_tid);
                if (unlikely(!succ))
                {
                    dsm->unreliable_commit_send();
                }
                // dsm->unreliable_send((char *) resp_rdma_buf,
                //                      sizeof(RespMsg),
                //                      from_nid,
                //                      from_tid);
            }
            dsm->unreliable_commit_send();
        }
    }
    LOG(INFO) << "[bench] server " << tid << " exiting.";
}

void server_burn(std::shared_ptr<DSM> dsm,
                 size_t total_msg_nr,
                 size_t thread_nr)
{
    std::vector<std::thread> threads;
    std::array<Perthread<std::atomic<size_t>>, MAX_MACHINE> gots;

    std::atomic<bool> finished{false};

    std::thread monitor([&gots, total_msg_nr, &finished, dsm]() {
        while (true)
        {
            std::this_thread::sleep_for(1s);
            size_t sum = 0;
            for (size_t m = 0; m < MAX_MACHINE; ++m)
            {
                for (size_t i = 0; i < gots[0].size(); ++i)
                {
                    sum += gots[m][i].load(std::memory_order_relaxed);
                }
            }
            if (sum >= total_msg_nr)
            {
                LOG(WARNING) << "[bench] Okay, server receives all the "
                                "messages. Exit...";
                finished = true;
                break;
            }
            else
            {
                LOG(INFO) << "[bench] got " << sum << " less than "
                          << total_msg_nr;
            }
        }
    });

    for (size_t i = 0; i < thread_nr - 1; ++i)
    {
        threads.emplace_back([dsm, &gots, &finished]() {
            dsm->registerThread();
            server_burn_do(dsm, finished, gots);
        });
    }
    server_burn_do(dsm, finished, gots);

    monitor.join();
    for (auto &t : threads)
    {
        t.join();
    }
}

void client(std::shared_ptr<DSM> dsm)
{
    LOG(INFO) << "Begin burn";
    client_burn(dsm, kClientThreadNr);

    dsm->keeper_barrier("finished", 100ms);
}
void server(std::shared_ptr<DSM> dsm)
{
    size_t expect_msg_nr =
        ::config::get_client_nids().size() * kClientThreadNr * kBurnCnt;
    LOG(INFO) << "Begin burn";
    server_burn(dsm, expect_msg_nr, kServerThreadNr);

    dsm->keeper_barrier("finished", 100ms);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    auto dsm = DSM::getInstance(config);

    sleep(1);

    dsm->registerThread();

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (::config::is_client(nid))
    {
        client(dsm);
    }
    else
    {
        server(dsm);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}