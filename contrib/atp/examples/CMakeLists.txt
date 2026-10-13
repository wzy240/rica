build_lib_example(
    NAME atp-v1-newtopo
    SOURCE_FILES atp-v1-newtopo.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-v1-newtopo-straggler
    SOURCE_FILES atp-v1-newtopo-straggler.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-single-layer
    SOURCE_FILES atp-single-layer.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-single-layer-resource
    SOURCE_FILES atp-single-layer-resource.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-spineleaf
    SOURCE_FILES atp-spineleaf.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-bursty-straggler
    SOURCE_FILES atp-bursty-straggler.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-bursty-straggler-robust
    SOURCE_FILES atp-bursty-straggler-robust.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME atp-stragglers
    SOURCE_FILES atp-stragglers.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME main-experiment
    SOURCE_FILES main-experiment.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)

build_lib_example(
    NAME four-job-four-ps-fanin
    SOURCE_FILES four-job-four-ps-fanin.cc
    LIBRARIES_TO_LINK ${libatp}
                      ${libinternet}
                      ${libapplications}
                      ${libpoint-to-point}
)
