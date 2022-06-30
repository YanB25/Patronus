#include <iostream>
#include <thread>

#include "ImageMagick-6/Magick++.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Magick::Image img;

    LOG(INFO) << "OK";
    return 0;
}