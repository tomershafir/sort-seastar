#/bin/bash
[[ -z $SSORT_BUILD_DIR ]] && _SSORT_BUILD_DIR='build' || _SSORT_BUILD_DIR=$SSORT_BUILD_DIR
[[ -z $SSORT_TESTDATA_PATH ]] && _SSORT_TESTDATA_PATH='test/testdata' || _SSORT_TESTDATA_PATH=$SSORT_TESTDATA_PATH
[[ -z $SSORT_FLAGS ]] && _SSORT_FLAGS='' || _SSORT_FLAG=$SSORT_FLAGS

SIG_TERM_EXIT_BASE=128
SIGINT=2
SIGINT_EXIT=$(($SIG_TERM_EXIT_BASE + $SIGINT))

clean_setup() {
    rm -rf $1 $2
}

for compressed in $_SSORT_TESTDATA_PATH/*.txt.gz; do
    decompressed=${compressed%".gz"} &&
    gunzip -c $compressed > $decompressed
    actual=${decompressed}.sorted
    for i in {1..10}; do
        echo "$compressed $i:" &&
        time -p $_SSORT_BUILD_DIR/ssort $SSORT_FLAGS $decompressed 2>/dev/null
        if [ $? -eq $SIGINT_EXIT ]; then
            echo "[ERROR] latency.sh: ssort child process was interrupted"
            clean_setup $decompressed $actual
            exit $SIGINT_EXIT
        fi
    done
    clean_setup $decompressed $actual
done
