/*
 * @Author: your name
 * @Date: 2020-09-04 15:22:11
 * @LastEditTime: 2020-09-04 15:31:01
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /Sherman/test/skiplist_test.cpp
 */
#include "Timer.h"
#include "inlineskiplist.h"

// Our test skip list stores 8-byte unsigned integers
typedef uint64_t Key;

// static const char *Encode(const uint64_t *key) {
//   return reinterpret_cast<const char *>(key);
// }

static Key Decode(const char *key)
{
    Key rv;
    memcpy(&rv, key, sizeof(Key));
    return rv;
}

struct TestComparator
{
    typedef Key DecodedType;

    static DecodedType decode_key(const char *b)
    {
        return Decode(b);
    }

    int operator()(const char *a, const char *b) const
    {
        if (Decode(a) < Decode(b))
        {
            return -1;
        }
        else if (Decode(a) > Decode(b))
        {
            return +1;
        }
        else
        {
            return 0;
        }
    }

    int operator()(const char *a, const DecodedType b) const
    {
        if (Decode(a) < b)
        {
            return -1;
        }
        else if (Decode(a) > b)
        {
            return +1;
        }
        else
        {
            return 0;
        }
    }
};

int main()
{
    Allocator alloc;
    TestComparator cmp;
    InlineSkipList<TestComparator> list(cmp, &alloc, 21);

    InlineSkipList<TestComparator>::Iterator iter(&list);

    const uint64_t Space = 100000ull;
    const int loop = 10000;
    for (uint64_t i = 0; i < Space; ++i)
    {
        auto buf = list.AllocateKey(sizeof(Key));
        *(Key *) buf = i;
        list.InsertConcurrently(buf);
    }

    Timer t;
    t.begin();
    for (int i = 0; i < loop; ++i)
    {
        uint64_t k = rand() % Space;
        iter.Seek((char *) &k);
    }
    t.end_print(loop);

    return 0;
}
