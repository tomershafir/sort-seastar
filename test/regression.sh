#/bin/bash
[[ -z $BUILD_DIR ]] && _BUILD_DIR='build' || _BUILD_DIR=$BUILD_DIR
[[ -z $TESTDATA_PATH ]] && _TESTDATA_PATH='test/testdata' || _TESTDATA_PATH=$TESTDATA_PATH

clean() {
    rm -rf $1 $2 $3
}

for compressed_raw in $_TESTDATA_PATH/*.txt.gz; do
    decompressed_raw=${compressed_raw%".gz"} &&
    gunzip -c $compressed_raw > $decompressed_raw &&

    compressed_sorted=${decompressed_raw}._sorted.gz &&
    decompressed_sorted=${compressed_sorted%".gz"} &&
    gunzip -c $compressed_sorted > $decompressed_sorted &&
    
    $_BUILD_DIR/ssort $decompressed_raw 2>/dev/null

    actual=${decompressed_raw}.sorted
    cmp -s $decompressed_sorted $actual
    if [ $? -ne 0 ]; then
        echo "Error: expected file and actual file are not equal."
        clean $decompressed_raw $decompressed_sorted
        exit 1
    fi
    clean $decompressed_raw $decompressed_sorted $actual
done
