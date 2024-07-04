use protobuf::Message;
use sketches_rust::{proto, DDSketch};

#[test]
pub fn test_proto() {
    let mut initial_sketch = DDSketch::unbounded_dense(0.01).unwrap();
    let test_data = vec![0.1, 0.5, 0.7, 0.9, 1.4, 3.1, 0.6, 2.5, 0.55, 1.34, 5.34, 0.4, -1.4];

    for val in test_data {
        initial_sketch.accept(val);
    }

    let min = initial_sketch.get_min().unwrap();
    let max = initial_sketch.get_max().unwrap();
    let p50 = initial_sketch.get_value_at_quantile(0.5).unwrap();

    let sketch_proto = proto::ddsketch::DDSketch::from(initial_sketch);
    let bytes = sketch_proto.write_to_bytes().unwrap();

    println!("Bytes count: {:?}", bytes.len());
    println!("Bytes: {:?}", bytes);

    let mut restored_sketch: DDSketch = proto::ddsketch::DDSketch::parse_from_bytes(&bytes).unwrap().into();

    assert_eq!(min, restored_sketch.get_min().unwrap());
    assert_eq!(max, restored_sketch.get_max().unwrap());
    assert_eq!(p50, restored_sketch.get_value_at_quantile(0.5).unwrap());
}
