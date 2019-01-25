# This is the web demo for showcasing healthcare Deid capabilities

load("@my_deps//:requirements.bzl", "requirement")

package(
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "config",
    srcs = ["config.py"],
)

py_binary(
    name = "model",
    srcs = ["model.py"],
    deps = [":model_lib"],
)

py_library(
    name = "model_lib",
    srcs = ["model.py"],
    deps = [":config"],
)

py_binary(
    name = "server",
    srcs = [
        "__init__.py",
        "server.py",
    ],
    deps = [
        ":config",
        ":model_lib",
        requirement("google-cloud-storage"),
        "//common:gcsutil",
        "//dlp:run_deid_lib",
        "//eval:run_pipeline_lib",
    ],
)
