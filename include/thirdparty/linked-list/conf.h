#pragma once
#ifndef PATRONUS_LINKED_LIST_CONFIG_H_
#define PATRONUS_LINKED_LIST_CONFIG_H_

namespace patronus::list
{
struct HandleConfig
{
    std::string name;
    bool bypass_prot{false};
    bool lock_free{true};
    size_t retry_nr{std::numeric_limits<size_t>::max()};

    std::string conf_name() const
    {
        return name;
    }
};

inline std::ostream &operator<<(std::ostream &os, const HandleConfig &config)
{
    os << "{HandleConfig name: " << config.name
       << ", bypass_prot: " << config.bypass_prot
       << ", lock_free: " << config.lock_free << "}";
    return os;
}

}  // namespace patronus::list
#endif