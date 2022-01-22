#include <algorithm>
#include <chrono>
#include <random>
#include <set>
#include <thread>
#include <utility>

#include "Common.h"
#include "patronus/LockManager.h"

using namespace define::literals;
constexpr static size_t kTestSize = 1_M;

void test_single_thread()
{
    patronus::LockManager<8, 1024> m;
    DCHECK_EQ(m.bucket_nr(), 8);
    DCHECK_GE(m.slot_nr(), 4096 * 8);

    std::set<std::pair<size_t, size_t>> book;

    for (size_t i = 0; i < kTestSize; ++i)
    {
        bool insert = rand() % 2 == 0;

        if (insert)
        {
            auto bucket_id = rand() % m.bucket_nr();
            auto slot_id = rand() % m.slot_nr();
            if (book.count({bucket_id, slot_id}) == 1)
            {
                DVLOG(1) << "Locking (" << bucket_id << ", " << slot_id << ")";
                // has
                CHECK(!m.try_lock(bucket_id, slot_id))
                    << "** locking twice should fail: " << bucket_id << ", "
                    << slot_id;
            }
            else
            {
                DVLOG(1) << "Locking (" << bucket_id << ", " << slot_id << ")";
                // do not have
                CHECK(m.try_lock(bucket_id, slot_id))
                    << "** locking an unlocked object should success: "
                    << bucket_id << ", " << slot_id;
                book.insert({bucket_id, slot_id});
            }
        }
        else
        {
            if (unlikely(book.empty()))
            {
                continue;
            }
            auto nth = rand() % book.size();
            auto it = book.begin();
            for (size_t t = 0; t < nth; ++t)
            {
                it++;
            }
            if (it == book.end())
            {
                LOG(FATAL) << "nth " << nth << " reach book.size() "
                           << book.size() << "< so it == book.end()";
            }
            auto bucket_id = it->first;
            auto slot_id = it->second;
            DVLOG(1) << "Unlocking (" << bucket_id << ", " << slot_id << ")";
            m.unlock(bucket_id, slot_id);
            book.erase(it);
        }
    }

    while (!book.empty())
    {
        auto it = book.begin();
        auto bucket_id = it->first;
        auto slot_id = it->second;
        m.unlock(bucket_id, slot_id);
        book.erase(it);
    }
}

void test_multithread(size_t thread_nr)
{
    patronus::LockManager<8, 1024> m;
    DCHECK_EQ(m.bucket_nr(), 8);
    DCHECK_GE(m.slot_nr(), 4096 * 8);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [&m]()
            {
                std::set<std::pair<size_t, size_t>> book;

                for (size_t i = 0; i < kTestSize; ++i)
                {
                    bool insert = rand() % 2 == 0;

                    if (insert)
                    {
                        auto bucket_id = rand() % m.bucket_nr();
                        auto slot_id = rand() % m.slot_nr();
                        if (book.count({bucket_id, slot_id}) == 1)
                        {
                            DVLOG(1) << "Locking (" << bucket_id << ", "
                                     << slot_id << ")";
                            // has
                            CHECK(!m.try_lock(bucket_id, slot_id))
                                << "** locking twice should fail: " << bucket_id
                                << ", " << slot_id;
                        }
                        else
                        {
                            DVLOG(1) << "Locking (" << bucket_id << ", "
                                     << slot_id << ")";
                            // may not have
                            bool succ = m.try_lock(bucket_id, slot_id);
                            if (succ)
                            {
                                book.insert({bucket_id, slot_id});
                            }
                        }
                    }
                    else
                    {
                        if (unlikely(book.empty()))
                        {
                            continue;
                        }
                        auto nth = rand() % book.size();
                        auto it = book.begin();
                        for (size_t t = 0; t < nth; ++t)
                        {
                            it++;
                        }
                        if (it == book.end())
                        {
                            LOG(FATAL)
                                << "nth " << nth << " reach book.size() "
                                << book.size() << "< so it == book.end()";
                        }
                        auto bucket_id = it->first;
                        auto slot_id = it->second;
                        DVLOG(1) << "Unlocking (" << bucket_id << ", "
                                 << slot_id << ")";
                        m.unlock(bucket_id, slot_id);
                        book.erase(it);
                    }
                }

                while (!book.empty())
                {
                    auto it = book.begin();
                    auto bucket_id = it->first;
                    auto slot_id = it->second;
                    m.unlock(bucket_id, slot_id);
                    book.erase(it);
                }
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Begin single thread tests";
    test_single_thread();
    LOG(INFO) << "Begin multiple thread tests";
    test_multithread(8);

    LOG(INFO) << "ALL TEST PASSED";
}