#include <algorithm>
#include <filesystem>
#include <random>
#include <vector>

#include "Common.h"
#include "DataFrame/DataFrame.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/DataFrameF.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace hmdf;

using StrDataFrame = StdDataFrame<std::string>;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<std::string> idx_col1 = {
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
    std::vector<int> int_col1 = {1, 2, -3, -4, 5, 6, 7, 8, 9, -10};
    std::vector<double> dbl_col1 = {
        0.01, 0.02, 0.03, 0.03, 0.05, 0.06, 0.03, 0.08, 0.09, 0.03};

    StrDataFrame ul_df1;
    auto f = gen_F_mul<int, double, double>();

    ul_df1.load_index(std::move(idx_col1));
    ul_df1.load_column<double>("dbl_col", std::move(dbl_col1));
    ul_df1.load_column<int>("integers", std::move(int_col1));
    ul_df1.consolidate<int, double, double>(
        "integers", "dbl_col", "multiple", f, false);

    ul_df1.write<std::ostream, std::string, double, int>(std::cout,
                                                         io_format::csv2);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    LOG(INFO) << "Generating csv to " << filename;
    ul_df1.write<std::string, double, int>(filename.c_str(), io_format::csv2);

    LOG(INFO) << "Meta is `" << FLAGS_exec_meta << "`";

    LOG(INFO) << std::filesystem::path(argv[0]).filename().string()
              << " finished. ctrl+C to quit.";
}