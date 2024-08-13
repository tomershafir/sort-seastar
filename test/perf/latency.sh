#/bin/bash
[[ -z $SSORT_BUILD_DIR ]] && _SSORT_BUILD_DIR='build' || _SSORT_BUILD_DIR=$SSORT_BUILD_DIR
[[ -z $SSORT_TESTDATA_PATH ]] && _SSORT_TESTDATA_PATH='test/testdata' || _SSORT_TESTDATA_PATH=$SSORT_TESTDATA_PATH
[[ -z $SSORT_FLAGS ]] && _SSORT_FLAGS='' || _SSORT_FLAG=$SSORT_FLAGS

for compressed in $_SSORT_TESTDATA_PATH/*.txt.gz; do
    decompressed=${compressed%".gz"} &&
    gunzip -c $compressed > $decompressed

    for i in {1..10}; do
        echo "$compressed $i:" &&
        time -p $_SSORT_BUILD_DIR/ssort $SSORT_FLAGS $decompressed 2>/dev/null
    done

    rm -rf $decompressed
done
