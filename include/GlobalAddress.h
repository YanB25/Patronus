#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"


class GlobalAddress {
public:
  uint64_t addr;
  uint8_t nodeID;

  static GlobalAddress Null() {
    static GlobalAddress zero{0, 0};
    return zero;
  }
} __attribute__((packed));

inline GlobalAddress GADD(const GlobalAddress &addr, int off) {
  auto ret = addr;
  ret.addr += off;
  return ret;
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return (lhs.nodeID == rhs.nodeID) && (lhs.addr == rhs.addr);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
  os << "[" << (int)obj.nodeID << ", " << obj.addr << "]";
  return os;
}

#endif /* __GLOBALADDRESS_H__ */
