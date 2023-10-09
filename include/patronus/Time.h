#ifndef PATRONUS_TIME_H_
#define PATRONUS_TIME_H_

#include <chrono>

namespace patronus::time
{
using ns_t = int64_t;
using term_t = int64_t;

/**
 * @brief PatronusTime is the adjusted time by TimeSyncer. Considered the time
 * drift in the cluster.
 *
 */
class PatronusTime
{
public:
    PatronusTime(ns_t ns, ns_t adjustment)
        : ns_(ns), adjustment_(adjustment), term_(ns + adjustment)
    {
    }
    PatronusTime(term_t term) : ns_(0), adjustment_(0), term_(term)
    {
    }
    term_t term() const
    {
        return term_;
    }
    bool operator<(const PatronusTime &rhs) const
    {
        return term_ < rhs.term_;
    }
    ns_t operator-(const PatronusTime &rhs) const
    {
        return term() - rhs.term();
    }
    static PatronusTime max()
    {
        return PatronusTime(std::numeric_limits<decltype(ns_)>::max(), 0);
    }

private:
    friend std::ostream &operator<<(std::ostream &, const PatronusTime &);

    ns_t ns_{0};
    ns_t adjustment_{0};
    term_t term_{0};
};
inline std::ostream &operator<<(std::ostream &os, const PatronusTime &t)
{
    os << "{PatronusTime term: " << t.term_ << ", base ns: " << t.ns_
       << ", adjust: " << t.adjustment_ << "}";
    return os;
}

}  // namespace patronus::time
#endif