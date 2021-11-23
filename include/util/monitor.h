#ifndef MONITOR_H_
#define MONITOR_H_
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>
namespace bench
{
class ColumnBase
{
public:
    virtual std::string get_printable_value() const = 0;
    virtual std::string name() const = 0;
    virtual ~ColumnBase() = default;
};
template <typename T>
class Column: public ColumnBase
{
public:
    Column(const std::string &name, std::atomic<T> *val)
        : name_(name), val_(val)
    {
    }
    std::string name() const override
    {
        return name_;
    }
    T get_value() const
    {
        return val_->load();
    }
    std::string get_printable_value() const override
    {
        return std::to_string(get_value());
    }

private:
    std::string name_;
    std::atomic<T>* val_;
};
class BenchResult
{
public:
    template <typename T>
    BenchResult &add_column(const std::string &col, std::atomic<T> *val)
    {
        auto pcolumn = future::make_unique<Column<T>>(col, val);
        columns_.emplace_back(std::move(pcolumn));
        return *this;
    }
    void snapshot()
    {
        std::vector<std::string> row;
        row.reserve(columns_.size());
        for (auto&& pcol: columns_)
        {
            row.push_back(pcol->get_printable_value());
        }
        buffered_output_.emplace_back(std::move(row));
    }
    void report(std::ostream &os)
    {
        for (size_t i = 0; i < columns_.size(); ++i)
        {
            os << columns_[i]->name();
            if (i + 1 < columns_.size())
            {
                os << ",";
            }
        }
        os << std::endl;

        for (const auto &row : buffered_output_)
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
    std::vector<std::unique_ptr<ColumnBase>> columns_;
    std::list<std::vector<std::string>> buffered_output_;
};
class BenchManager
{
public:
    BenchManager &operator=(const BenchManager &) = delete;
    BenchManager(const BenchManager &) = delete;
    BenchManager() = default;
    static BenchManager &ins()
    {
        static BenchManager ins_;
        return ins_;
    }
    BenchResult &reg(const std::string &name)
    {
        std::lock_guard<std::mutex> lk(mu_);
        return bench_result_[name];
    }
    void report(const std::string &name, std::ostream &os = std::cout)
    {
        std::lock_guard<std::mutex> lk(mu_);
        bench_result_[name].report(os);
    }
    void to_csv(const std::string &name, std::string location_dir="./")
    {
        std::fstream fout;
        if (!location_dir.empty() && location_dir[location_dir.size() -1] != '/')
        {
            location_dir += "/";
        }
        fout.open(location_dir + name + ".csv", std::ios::out | std::ios::trunc);
        report(name, fout);
        fout.close();
    }

private:
    std::unordered_map<std::string, BenchResult> bench_result_;
    std::mutex mu_;
};
}  // namespace bench
#endif