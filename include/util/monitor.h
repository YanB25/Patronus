#pragma once
#ifndef MONITOR_H_
#define MONITOR_H_

#include <atomic>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "Common.h"
namespace bench
{
class ColumnBase
{
public:
    virtual std::string get_printable_value() const = 0;
    virtual std::string name() const = 0;
    virtual std::string print_name() const = 0;
    virtual double get_value() const = 0;
    virtual ~ColumnBase() = default;
    virtual void clear() = 0;
};
template <typename T>
class Column : public ColumnBase
{
public:
    Column(const std::string &name,
           const std::string &print_name,
           std::atomic<T> *val)
        : name_(name), print_name_(print_name), val_(val)
    {
    }
    std::string name() const override
    {
        return name_;
    }
    std::string print_name() const override
    {
        return print_name_;
    }
    double get_value() const override
    {
        return val_->load();
    }
    std::string get_printable_value() const override
    {
        return std::to_string(get_value());
    }
    void clear() override
    {
        val_->store({});
    }

private:
    std::string name_;
    std::string print_name_;
    std::atomic<T> *val_;
};
class DependentColumn : public ColumnBase
{
public:
    using Transformer = std::function<double(double)>;
    DependentColumn(const std::string &name,
                    const std::string &print_name,
                    ColumnBase *dep,
                    Transformer t)
        : name_(name), print_name_(print_name), dep_(dep), t_(t)
    {
    }
    std::string name() const override
    {
        return name_;
    }
    std::string print_name() const override
    {
        return print_name_;
    }
    double get_value() const override
    {
        return t_(dep_->get_value());
    }
    std::string get_printable_value() const override
    {
        return std::to_string(get_value());
    }
    void clear() override
    {
    }

private:
    std::string name_;
    std::string print_name_;
    ColumnBase *dep_;
    Transformer t_;
};
class BenchResult
{
public:
    template <typename T>
    BenchResult &add_column(const std::string &col, std::atomic<T> *val)
    {
        return add(col, col, val);
    }
    template <typename T>
    BenchResult &add_column_ns(const std::string &col, std::atomic<T> *val)
    {
        return add(col, col + " (ns)", val);
    }
    template <typename T>
    BenchResult &add_column_ops(const std::string &col, std::atomic<T> *val)
    {
        return add(col, col + " (ops)", val);
    }
    /**
     * @param depend_col the name of the referenced latency column in
     * nanosecond.
     * @return BenchResult&
     */
    BenchResult &add_dependent_throughput(const std::string &depend_col)
    {
        auto *inner = search(depend_col);
        if (inner == nullptr)
        {
            throw std::runtime_error("Can not found dependent column: `" +
                                     depend_col + "`");
        }
        auto pcolumn = future::make_unique<DependentColumn>(
            inner->name(), inner->name() + " (ops)", inner, [](double input) {
                return input == 0 ? 0 : 1e9 / input;
            });
        columns_.emplace_back(std::move(pcolumn));
        return *this;
    }
    /**
     * @brief calculate the latency from a column
     *
     * @param depend_col the referenced column for ops.
     * @return BenchResult&
     */
    BenchResult &add_dependent_latency(const std::string &depend_col)
    {
        auto *inner = search(depend_col);
        if (inner == nullptr)
        {
            throw std::runtime_error("Can not found dependent column: `" +
                                     depend_col + "`");
        }
        auto pcolumn = future::make_unique<DependentColumn>(
            inner->name(), inner->name() + " (ns)", inner, [](double input) {
                return input == 0 ? 0 : 1e9 / input;
            });
        return *this;
    }
    void snapshot()
    {
        std::vector<std::string> row;
        row.reserve(columns_.size());
        for (auto &&pcol : columns_)
        {
            row.push_back(pcol->get_printable_value());
        }
        buffered_output_.emplace_back(std::move(row));
    }
    void report(std::ostream &os)
    {
        for (size_t i = 0; i < columns_.size(); ++i)
        {
            os << columns_[i]->print_name();
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
    void clear()
    {
        for (auto &&column : columns_)
        {
            column->clear();
        }
    }

private:
    template <typename T>
    BenchResult &add(const std::string &name,
                     const std::string &print_name,
                     std::atomic<T> *val)
    {
        auto pcolumn = future::make_unique<Column<T>>(name, print_name, val);
        columns_.emplace_back(std::move(pcolumn));
        return *this;
    }
    ColumnBase *search(const std::string &name)
    {
        for (auto &&column : columns_)
        {
            if (column->name() == name)
            {
                return column.get();
            }
        }
        return nullptr;
    }
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
    void to_csv(const std::string &name, std::string location_dir = "./")
    {
        std::fstream fout;
        if (!location_dir.empty() &&
            location_dir[location_dir.size() - 1] != '/')
        {
            location_dir += "/";
        }
        fout.open(location_dir + name + ".csv",
                  std::ios::out | std::ios::trunc);
        report(name, fout);
        fout.close();
    }

private:
    std::unordered_map<std::string, BenchResult> bench_result_;
    std::mutex mu_;
};
}  // namespace bench
#endif