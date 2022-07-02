#include <filesystem>
#include <iostream>
#include <thread>

#include "ImageMagick-6/Magick++.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/Tracer.h"
#include "util/Util.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    util::TraceManager tm(1);
    auto trace = tm.trace("img");

    auto img_path = artifacts_directory() / "view.thumbnail.jpeg";
    Magick::Image img(img_path);
    trace.pin("ctor");

    Magick::Blob blob;
    img.write(&blob);
    trace.pin("dump to blob");

    LOG(INFO) << "The image column: " << img.columns()
              << ", row: " << img.rows() << ", blob size: " << blob.length()
              << ", at " << blob.data();
    Magick::Blob blob2 = blob;
    trace.pin("blob2 = blob");
    Magick::Image img2(blob2);
    trace.pin("ctor(blob)");

    // yes, this write can work.
    // img2.write(artifacts_directory() / "view2.jpeg");
    auto img_zoom = img;
    trace.pin("cpy");
    img.zoom({size_t(img.columns() * 0.1), size_t(img.rows() * 0.1)});
    // img.chop({size_t(img.columns() * 0.01), size_t(img.rows() * 0.01)});
    // img.write(artifacts_directory() / ("view.thumbnail.5.jpeg"));

    trace.pin("chop");
    LOG(INFO) << trace;

    return 0;
}