use std::collections::HashMap;

pub fn normalize_ranks(ranks: &HashMap<u64, f64>, normalized_ranks: &mut HashMap<u64, f64>) {
    let num_of_nodes = ranks.len();
    let rank_sum: f64 = ranks.values().sum();
    let correction_factor = num_of_nodes as f64 / rank_sum;
    for (vertex_id, ranks) in ranks.iter() {
        normalized_ranks.insert(*vertex_id, *ranks * correction_factor);
    }
}

pub fn topk_ranks(ranks: &HashMap<u64, f64>, topked_ranks: &mut Vec<(u64, f64)>, topk: usize) {
    let mut ranks_vec = Vec::from_iter(ranks.iter());
    ranks_vec.sort_by(|(_, rank_a), (_, rank_b)| rank_b.partial_cmp(rank_a).unwrap());
    let topk = topk.min(ranks_vec.len());
    for (vertex_id, rank) in ranks_vec.drain(..topk) {
        topked_ranks.push((*vertex_id, *rank));
    }
}

pub fn display_ranks(ranks: &Vec<(u64, f64)>) {
    for (vertex_id, rank) in ranks.iter() {
        println!("vertex_id: {}\trank: {:.5}", vertex_id, rank);
    }
}
