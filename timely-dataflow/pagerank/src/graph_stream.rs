use std::fs::File;
use std::io::{prelude::*, BufReader};
use timely::dataflow::operators::input::Handle;
use timely::progress::Timestamp;

pub fn from_edge_list_file(
    path: impl AsRef<std::path::Path>,
    edge_stream_handle: &mut Handle<impl Timestamp, (u64, u64)>,
) {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(line) = line {
            let fields = line
                .split_whitespace()
                .map(|s| s.parse::<u64>().unwrap())
                .take(2)
                .collect::<Vec<u64>>();
            let src_id = fields[0];
            let dst_id = fields[1];
            edge_stream_handle.send((src_id, dst_id));
        }
    }
}

pub fn from_vertex_list_file(
    path: impl AsRef<std::path::Path>,
    vertex_stream_handle: &mut Handle<impl Timestamp, u64>,
) {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(line) = line {
            let vertex_id = line.parse::<u64>().unwrap();
            vertex_stream_handle.send(vertex_id);
        }
    }
}
