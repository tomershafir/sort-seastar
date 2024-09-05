#/bin/bash
[[ -z $SSORT_BUILD_DIR ]] && _SSORT_BUILD_DIR='build' || _SSORT_BUILD_DIR=$SSORT_BUILD_DIR
[[ -z $SSORT_TESTDATA_PATH ]] && _SSORT_TESTDATA_PATH='test/testdata' || _SSORT_TESTDATA_PATH=$SSORT_TESTDATA_PATH

SIG_TERM_EXIT_BASE=128
SIGINT=2
SIGINT_EXIT=$(($SIG_TERM_EXIT_BASE + $SIGINT))

clean_setup() {
    rm -rf $1 $2
}

clean() {
    rm -rf $1 $2 $3
}

for compressed_raw in $_SSORT_TESTDATA_PATH/*.txt.gz; do
    decompressed_raw=${compressed_raw%".gz"} &&
    gunzip -c $compressed_raw > $decompressed_raw &&

    compressed_sorted=${decompressed_raw}._sorted.gz &&
    decompressed_sorted=${compressed_sorted%".gz"} &&
    gunzip -c $compressed_sorted > $decompressed_sorted &&
    
    actual=${decompressed_raw}.sorted
    $_SSORT_BUILD_DIR/ssort $decompressed_raw 2>/dev/null
    ssort_code=$?
    if [ $ssort_code -eq $SIGINT_EXIT ]; then
        echo "[INFO] regression.sh: ssort child process was interrupted"
        clean_setup $decompressed_raw $decompressed_sorted
        exit $ssort_code
    fi
    if [ $ssort_code -ne 0 ]; then
        echo "[ERROR] regression.sh: ssort child process failed with exit code $ssort_code"
        clean_setup $decompressed_raw $decompressed_sorted
        exit $ssort_code
    fi

    cmp -s $decompressed_sorted $actual
    if [ $? -ne 0 ]; then
        echo "[ERROR] regression.sh: expected file ${decompressed_sorted} and actual file ${actual} are not equal."
        clean $decompressed_raw $decompressed_sorted $actual
        exit 1
    fi
    echo "[INFO] regression.sh: test passed - ${actual}."
    clean $decompressed_raw $decompressed_sorted $actual
done
