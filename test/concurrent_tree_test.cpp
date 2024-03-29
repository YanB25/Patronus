#include <thread>

#include "DSM.h"
#include "Tree.h"

constexpr uint64_t kTestKeySpace = 100000000ull;
constexpr int kNodeNum = 1;
constexpr int kThreadNum = 1;

Tree *tree;
std::shared_ptr<DSM> dsm;

void test(int id)
{
    if (dsm->getMyNodeID() == 0)
    {
        while (true)
            ;
    }

    dsm->registerThread();

    int mod = dsm->getMyNodeID() * kThreadNum + id;
    int all_thread = kNodeNum * kThreadNum;

    Value v;
    for (uint64_t i = 1; i < kTestKeySpace; ++i)
    {
        if (i % all_thread == (uint64_t) mod)
        {
            tree->insert(i, i * 2);
            auto res = tree->search(i, v);

            if (!res || v != i * 2)
            {
                printf("Error %ld\n", i);
            }
            assert(res && v == i * 2);
        }
        // if (i % 1000000 == 0) {
        //   printf("%d\n", i);
        // }
    }
}

int main()
{
    DSMConfig config;
    config.machineNR = kNodeNum;
    dsm = DSM::getInstance(config);

    dsm->registerThread();
    tree = new Tree(dsm);

    // Value v;

    // test(0);

    std::thread th[kThreadNum];
    for (int i = 0; i < kThreadNum; ++i)
    {
        th[i] = std::thread(test, i);
    }

    for (int i = 0; i < kThreadNum; ++i)
    {
        th[i].join();
    }

    // tree->print_and_check_tree();

    //   for (uint64_t i = 10240 - 1; i >= 1; --i) {
    //     tree->insert(i, i * 3);
    //   }

    //   for (uint64_t i = 1; i < 10240; ++i) {
    //     auto res = tree->search(i, v);
    //     assert(res && v == i * 3);
    //     // std::cout << "search result:  " << res << " v: " << v <<
    // std::endl;
    //   }

    //   for (uint64_t i = 10240 - 1; i >= 1; --i) {
    //     tree->del(i);
    //   }

    //   for (uint64_t i = 1; i < 10240; ++i) {
    //     auto res = tree->search(i, v);
    //     assert(!res);
    //     // std::cout << "search result:  " << res << " v: " << v <<
    // std::endl;
    //   }

    //   auto res = tree->search(23333, v);
    //   assert(!res);

    printf("Hello\n");

    while (true)
        ;
}