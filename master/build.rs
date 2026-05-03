fn main() {
    tonic_build::compile_protos("../proto/proto/mapreduce.proto").expect("failed to compile proto");
}
