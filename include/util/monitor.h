#ifndef MONITOR_H_
#define MONITOR_H_
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
namespace bench
{
class BenchResult
{
public:
    BenchResult& add_column(const std::string& col, std::atomic<double>* val)
    {
        columns_.push_back(col);
        values_.push_back(val);
        return *this;
    }
    void snapshot()
    {
        std::vector<double> row;
        row.reserve(columns_.size());
        for (auto* v: values_)
        {
            row.push_back(v->load());
        }
        buffered_values_.emplace_back(std::move(row));
    }
    void report(std::ostream& os)
    {
        for (size_t i = 0; i < columns_.size(); ++i)
        {
            os << columns_[i];
            if (i + 1 < columns_.size())
            {
                os << ",";
            }
        }
        os << std::endl;

        for (const auto& row: buffered_values_)
        {
            for (size_t i = 0; i < row.size(); ++i)
            {
                os << row[i];
                if (i + 1 < columns_.size())
                {
                    os << ",";
                }
            }
            os << std::endl;
        }
    }
private:
    std::vector<std::string> columns_;
    std::vector<std::atomic<double>*> values_;
    std::vector<std::vector<double>> buffered_values_;
};
class BenchManager
{
public:
    BenchManager& operator=(const BenchManager&) = delete;
    BenchManager(const BenchManager&) = delete;
    BenchManager() = default;
    static BenchManager& ins()
    {
        static BenchManager ins_;
        return ins_;
    }
    BenchResult& reg(const std::string& name)
    {
        std::lock_guard<std::mutex> lk(mu_);
        return bench_result_[name];
    }
    void report(const std::string& name, std::ostream& os = std::cout)
    {
        std::lock_guard<std::mutex> lk(mu_);
        bench_result_[name].report(os);
    }
    void to_csv(const std::string& name)
    {
        std::fstream fout;
        fout.open(name + ".csv", std::ios::out | std::ios::trunc);
        report(name, fout);
        fout.close();
    }
private:
    std::unordered_map<std::string, BenchResult> bench_result_;
    std::mutex mu_;
};
}
#endif