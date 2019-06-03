# This is the web demo for showcasing healthcare Deid capabilities

load("@my_deps//:requirements.bzl", "requirement")

package(
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])  # Apache License 2.0

py_library(
    name = "config",
    srcs = ["config.py"],
    srcs_version = "PY2AND3",
)

py_binary(
    name = "model",
    srcs = ["model.py"],
    python_version = "PY3",
    srcs_version = "PY2AND3",
    deps = [":model_lib"],
)

py_library(
    name = "model_lib",
    srcs = ["model.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":config",
        requirement("flask"),
        requirement("werkzeug"),
        requirement("itsdangerous"),
        requirement("click"),
        requirement("jinja2"),
        requirement("markupsafe"),
        requirement("sqlalchemy"),
        requirement("flask_sqlalchemy"),
        requirement("pymysql"),
    ],
)

py_binary(
    name = "server",
    srcs = [
        "__init__.py",
        "server.py",
    ],
    data = [
        "sample_deid_config.json",
    ],
    python_version = "PY3",
    srcs_version = "PY2AND3",
    deps = [
        ":config",
        ":model_lib",
        requirement("flask"),
        requirement("werkzeug"),
        requirement("itsdangerous"),
        requirement("click"),
        requirement("jinja2"),
        requirement("markupsafe"),
        requirement("google-cloud-core"),
        requirement("google-cloud-storage"),
        "//common:gcsutil",
        "//common:unicodecsv",
        "//dlp:run_deid_lib",
        "//eval:run_pipeline_lib",
        requirement("jsonschema"),
        requirement("functools32"),
        requirement("pyrsistent"),
        requirement("attrs"),
    ],
)
