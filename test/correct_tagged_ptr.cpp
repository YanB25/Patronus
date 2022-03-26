#include <algorithm>
#include <random>

#include "Common.h"
#include "glog/logging.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;

constexpr static size_t kTestTime = 1_M;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void validate_utagged_ptr(const UTaggedPtr ptr)
{
    uint8_t u8_h = ptr.u8_h;
    uint8_t u8_l = ptr.u8_l;
    uint16_t u16 = ptr.u16;
    uint16_t cal_u16 = ((uint64_t) u8_h << 8) | u8_l;
    CHECK_EQ(cal_u16, u16) << "Expect u16 == {u8_h, u8_l}. u8_h: " << std::hex
                           << (int) u8_h << ", u8_l: " << (int) u8_l
                           << ", cal_16: " << cal_u16 << ", u16: " << u16;

    uint8_t got_u8_h = ptr.val >> 56;
    uint16_t got_u16 = ptr.val >> 48;
    uint8_t got_u8_l = (got_u16 << 8) >> 8;
    CHECK_EQ(got_u8_h, u8_h)
        << std::hex << "Expect ptr.val >> 56 == ptr.u8_h. u8_h: " << (int) u8_h
        << ", u8_l: " << (int) u8_l << ", u16: " << (int) u16
        << ", got_u8_h: " << (int) got_u8_h << ", got_u16: " << (int) got_u16
        << ", got_u8_l: " << (int) got_u8_l;
    CHECK_EQ(got_u8_l, u8_l)
        << "Expect ptr.u8_l at lower u_8. u8_h: " << std::hex << (int) u8_h;
}

void validate_tagged_ptr(const TaggedPtr &ptr,
                         uint8_t expect_u8_h,
                         uint8_t expect_u8_l,
                         void *expect_ptr)
{
    CHECK_EQ(ptr.ptr(), expect_ptr);
    CHECK_EQ(ptr.u8_h(), expect_u8_h);
    CHECK_EQ(ptr.u8_l(), expect_u8_l);
}

void test_tagged_ptr()
{
    uint8_t expect_u8_h = 0;
    uint8_t expect_u8_l = 0;
    uint64_t *expect_ptr = (uint64_t *) malloc(64);
    TaggedPtr ptr(expect_ptr);
    for (size_t i = 0; i < kTestTime; ++i)
    {
        validate_tagged_ptr(ptr, expect_u8_h, expect_u8_l, expect_ptr);

        expect_u8_h = fast_pseudo_rand_int();
        ptr.set_u8_h(expect_u8_h);
        validate_tagged_ptr(ptr, expect_u8_h, expect_u8_l, expect_ptr);

        expect_u8_l = fast_pseudo_rand_int();
        ptr.set_u8_l(expect_u8_l);
        validate_tagged_ptr(ptr, expect_u8_h, expect_u8_l, expect_ptr);

        free(expect_ptr);
        expect_ptr = (uint64_t *) malloc(64);
        ptr.set_ptr(expect_ptr);
        validate_tagged_ptr(ptr, expect_u8_h, expect_u8_l, expect_ptr);

        // auto val = ptr.val();
        auto expect_ptr = ptr;
        auto origin_ptr = ptr;
        auto remember_expect_ptr = expect_ptr;
        TaggedPtr new_ptr;
        new_ptr.set_val(fast_pseudo_rand_int());
        auto remember_new_ptr = new_ptr;
        CHECK(ptr.cas(expect_ptr, new_ptr));
        CHECK_EQ(expect_ptr, remember_expect_ptr);
        CHECK(ptr.cas(new_ptr, remember_expect_ptr));
        CHECK_EQ(new_ptr, remember_new_ptr);
        CHECK_EQ(ptr, origin_ptr);
    }
    free(expect_ptr);
}

void test_utagged_ptr()
{
    UTaggedPtr ptr;
    for (size_t i = 0; i < kTestTime; ++i)
    {
        uint64_t expect_rand = fast_pseudo_rand_int();

        uint64_t *raw_ptr = (uint64_t *) malloc(64);
        *raw_ptr = expect_rand;

        ptr.val = (uint64_t) raw_ptr;
        validate_utagged_ptr(ptr);

        uint8_t new_u8_h = fast_pseudo_rand_int();
        uint8_t new_u8_l = fast_pseudo_rand_int();
        ptr.u8_h = new_u8_h;
        ptr.u8_l = new_u8_l;
        validate_utagged_ptr(ptr);

        uint16_t new_u16 = fast_pseudo_rand_int();
        ptr.u16 = new_u16;
        CHECK_EQ(ptr.u8_h, (uintptr_t) ptr.val >> 56)
            << std::hex
            << "Expect u8_h at higher 8 bit. new_u16: " << (int) new_u16
            << ", ptr.u8_h: " << (int) ptr.u8_h
            << ", ptr.val>>56: " << (int) (ptr.val >> 56);

        uint8_t tested_u8_l = (ptr.u16 << 8) >> 8;
        CHECK_EQ(ptr.u8_l, tested_u8_l)
            << std::hex
            << "Expect u8_l at lower 8 bit. ptr.u8_l: " << (int) ptr.u8_l
            << ", ptr.u16: " << (int) ptr.u16
            << ", lower of ptr.u16: " << (int) tested_u8_l;

        uint64_t *got_back_ptr =
            (uint64_t *) (((intptr_t) ptr.val << 16) >> 16);
        CHECK_EQ(got_back_ptr, raw_ptr);
        CHECK_EQ(*got_back_ptr, expect_rand);

        free(raw_ptr);
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test_utagged_ptr();
    test_tagged_ptr();

    LOG(INFO) << "Now, see the correctness of hash functions";
    auto hash = fast_pseudo_rand_int();
    auto m = hash_m(hash);
    auto fp = hash_fp(hash);
    auto h1 = hash_1(hash);
    auto h2 = hash_2(hash);
    LOG(INFO) << std::hex << "hash: " << hash << ", m: " << m << ", fp: " << fp
              << ", h1: " << h1 << ", h2: " << h2;

    LOG(INFO) << "finished. ctrl+C to quit.";
}