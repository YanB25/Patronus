#include "util/monitor.h"
int main()
{
    std::atomic<bool> b{false};
    std::atomic<size_t> u{100};
    bench::Column<bool> cb("cb", "cb", &b);
    bench::Column<size_t> cu("cu", "cu", &u);
    printf("%s\n", cb.get_printable_value().c_str());
    printf("%s\n", cu.get_printable_value().c_str());

}