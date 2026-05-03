fn main() {
    tonic_build::compile_protos("proto/mapreduce.proto").expect("failed to compile proto");
}
