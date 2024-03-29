#include <filesystem>
#include <iostream>
#include <thread>

#include "ImageMagick-6/Magick++.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/Tracer.h"
#include "util/Util.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

void bench()
{
    util::TraceManager tm(1);
    auto trace = tm.trace("img");

    auto img_path = artifacts_directory() / "view.thumbnail.jpeg";
    Magick::Image img(img_path);
    trace.pin("ctor");

    Magick::Blob blob;
    img.write(&blob);
    trace.pin("dump to blob");

    auto g = img.size();
    LOG(INFO) << "The image column: " << img.columns()
              << ", row: " << img.rows() << ", blob size: " << blob.length()
              << ", at " << blob.data()
              << ". multiple: " << img.columns() * img.rows()
              << ", img.size(): (" << img.size().width() << ", "
              << img.size().height()
              << ") , img.file_size(): " << img.fileSize();
    Magick::Blob blob2 = blob;
    trace.pin("blob2 = blob");
    Magick::Image img2(blob2);
    trace.pin("ctor(blob)");

    // yes, this write can work.
    // img2.write(artifacts_directory() / "view2.jpeg");
    auto img_zoom = img;
    trace.pin("cpy");
    // img.thumbnail?
    img.zoom({size_t(img.columns() * 0.1), size_t(img.rows() * 0.1)});
    // img.chop({size_t(img.columns() * 0.01), size_t(img.rows() * 0.01)});
    // img.write(artifacts_directory() / ("view.thumbnail.5.jpeg"));

    trace.pin("chop");
    LOG(INFO) << trace;
}

char *g_buf = nullptr;
size_t g_length = 0;

void test()
{
    util::TraceManager tm(1);
    auto trace = tm.trace("img");

    {
        auto img_path = artifacts_directory() / "view.thumbnail.jpeg";
        Magick::Image img(img_path);
        trace.pin("ctor");

        Magick::Blob blob;
        img.write(&blob);
        trace.pin("dump to blob");

        g_buf = (char *) malloc(blob.length());
        g_length = blob.length();
        memcpy(g_buf, blob.data(), blob.length());
        trace.pin("memcpy");
    }

    trace.pin("dctor of previous image.");

    {
        Magick::Blob blob2(g_buf, g_length);
        trace.pin("blob ctor");
        Magick::Image img2(blob2);
        trace.pin("img ctor");
        LOG(INFO) << "Succeeded. img2: " << img2.fileSize();
        LOG(INFO) << "blob2 length: " << g_length << ", checksum: "
                  << (void *) util::djb2_digest(blob2.data(), blob2.length());

        free(g_buf);
    }
    LOG(INFO) << trace;

    LOG(INFO) << "OKAY!";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // bench();
    test();

    return 0;
}