#include "inlineskiplist.h"

// Our test skip list stores 8-byte unsigned integers
typedef uint64_t Key;

// static const char *Encode(const uint64_t *key) {
//   return reinterpret_cast<const char *>(key);
// }

static Key Decode(const char *key) {
  Key rv;
  memcpy(&rv, key, sizeof(Key));
  return rv;
}

struct TestComparator {
  typedef Key DecodedType;

  static DecodedType decode_key(const char *b) { return Decode(b); }

  int operator()(const char *a, const char *b) const {
    if (Decode(a) < Decode(b)) {
      return -1;
    } else if (Decode(a) > Decode(b)) {
      return +1;
    } else {
      return 0;
    }
  }

  int operator()(const char *a, const DecodedType b) const {
    if (Decode(a) < b) {
      return -1;
    } else if (Decode(a) > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

int main() {
  Allocator alloc;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &alloc);

  auto buf = list.AllocateKey(sizeof(Key));
  *(Key *)buf = 123;
  list.InsertConcurrently(buf);

  InlineSkipList<TestComparator>::Iterator iter(&list);



  buf = list.AllocateKey(sizeof(Key));
  *(Key *)buf = 145;
  bool res = list.InsertConcurrently(buf);

    uint64_t k = 124;
  iter.Seek((char *)&k);
  if (iter.Valid()) {
    const char *val = iter.key();
    printf("%ld\n", *(uint64_t *)val);
  }

  return 0;
}
