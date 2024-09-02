#/bin/bash
[[ -z $SSORT_BUILD_DIR ]] && _SSORT_BUILD_DIR='build' || _SSORT_BUILD_DIR=$SSORT_BUILD_DIR
[[ -z $SSORT_TESTDATA_PATH ]] && _SSORT_TESTDATA_PATH='test/testdata' || _SSORT_TESTDATA_PATH=$SSORT_TESTDATA_PATH

clean() {
    rm -rf $1 $2 $3
}

for compressed_raw in $_SSORT_TESTDATA_PATH/*.txt.gz; do
    decompressed_raw=${compressed_raw%".gz"} &&
    gunzip -c $compressed_raw > $decompressed_raw &&

    compressed_sorted=${decompressed_raw}._sorted.gz &&
    decompressed_sorted=${compressed_sorted%".gz"} &&
    gunzip -c $compressed_sorted > $decompressed_sorted &&
    
    $_SSORT_BUILD_DIR/ssort $decompressed_raw 2>/dev/null

    actual=${decompressed_raw}.sorted
    cmp -s $decompressed_sorted $actual
    if [ $? -ne 0 ]; then
        echo "Error: expected file ${decompressed_sorted} and actual file ${actual} are not equal."
        clean $decompressed_raw $decompressed_sorted
        exit 1
    fi
    echo "Success: 0 errors found for ${actual}."
    clean $decompressed_raw $decompressed_sorted $actual
done
