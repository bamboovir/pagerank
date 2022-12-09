use pagerank::graph_stream;
use pagerank::rank_utils;
use std::collections::HashMap;
use std::path::Path;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{
    BranchWhen, Concat, ConnectLoop, Enter, Leave, LoopVariable, Map, Probe,
};
use timely::dataflow::Scope;
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    // tolerance = 1e-10 damping_factor = 0.85 year_range = [1992, 2002]
    let _iteration_times_to_convergence: Vec<usize> =
        vec![5, 28, 45, 46, 48, 48, 48, 50, 50, 50, 50];
    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let mut vertex_stream_handler = InputHandle::new();
        let mut edge_stream_handler = InputHandle::new();

        let mut year_probe = ProbeHandle::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let vertex_stream = vertex_stream_handler
                .to_stream(scope)
                .map(|vertex_id| (vertex_id, 0u64));

            let edge_stream = edge_stream_handler.to_stream(scope).concat(&vertex_stream);

            scope
                .iterative::<usize, _, _>(|subscope| {
                    let (rank_iter_handler, rank_iter_stream) = subscope.loop_variable(1);

                    let rank_stream = edge_stream.enter(&subscope).binary_frontier(
                        &rank_iter_stream,
                        Exchange::new(|(src_id, _dst_id)| *src_id),
                        Exchange::new(|(vertex_id, _contribution): &(u64, f64)| *vertex_id),
                        "PageRank",
                        |_capability, _info| {
                            let mut graph: HashMap<u64, Vec<u64>> = HashMap::new();
                            let mut ranks: HashMap<u64, f64> = HashMap::new();

                            let mut edge_stash = HashMap::new();
                            let mut contribution_stash = HashMap::new();

                            move |edge_input, contribution_input, output| {
                                while let Some((time, data)) = edge_input.next() {
                                    edge_stash
                                        .entry(time.retain())
                                        .or_insert(Vec::new())
                                        .extend(data.replace(Vec::new()));
                                }

                                while let Some((time, data)) = contribution_input.next() {
                                    contribution_stash
                                        .entry(time.retain())
                                        .or_insert(Vec::new())
                                        .extend(data.replace(Vec::new()));
                                }

                                let frontiers =
                                    &[edge_input.frontier(), contribution_input.frontier()];

                                for (time, edge_changes) in edge_stash.iter_mut() {
                                    if frontiers.iter().all(|f| !f.less_equal(time)) {
                                        println!(
                                            "e outer time = {} inner time = {}",
                                            time.time().outer,
                                            time.time().inner
                                        );
                                        let mut session = output.session(time);
                                        for (src_id, dst_id) in edge_changes.drain(..) {
                                            if dst_id == 0 {
                                                let vertex_id = src_id;
                                                graph.entry(vertex_id).or_insert(Vec::new());
                                            } else {
                                                graph
                                                    .entry(src_id)
                                                    .or_insert(Vec::new())
                                                    .push(dst_id);
                                            }
                                        }

                                        for vertex_id in graph.keys() {
                                            *ranks.entry(*vertex_id).or_insert(1.0f64) = 1.0f64;
                                        }

                                        for (vertex_id, rank) in ranks.iter() {
                                            session.give((*vertex_id, *rank));
                                        }
                                    }
                                }

                                edge_stash.retain(|_time, vals| !vals.is_empty());

                                for (time, contribution_changes) in contribution_stash.iter_mut() {
                                    if frontiers.iter().all(|f| !f.less_equal(time)) {
                                        println!(
                                            "c outer time = {} inner time = {}",
                                            time.time().outer,
                                            time.time().inner
                                        );
                                        let mut session = output.session(time);

                                        let mut contributions: HashMap<u64, f64> = HashMap::new();

                                        for (vertex_id, contribution) in
                                            contribution_changes.drain(..)
                                        {
                                            *contributions.entry(vertex_id).or_insert(0f64) +=
                                                contribution;
                                        }

                                        for (_vertex_id, contribution) in contributions.iter_mut() {
                                            *contribution = 0.15 + 0.85 * *contribution;
                                        }

                                        for (vertex_id, rank) in ranks.iter_mut() {
                                            *rank = contributions[vertex_id];
                                        }

                                        let iteration_times = 10;

                                        if time.inner >= iteration_times {
                                            let mut normalized_ranks: HashMap<u64, f64> =
                                                HashMap::new();
                                            rank_utils::normalize_ranks(
                                                &ranks,
                                                &mut normalized_ranks,
                                            );
                                            let mut topked_ranks = vec![];
                                            rank_utils::topk_ranks(
                                                &normalized_ranks,
                                                &mut topked_ranks,
                                                10,
                                            );
                                            rank_utils::display_ranks(&topked_ranks);
                                        } else {
                                            {
                                                let mut contributions: HashMap<u64, f64> =
                                                    HashMap::new();

                                                for vertex_id in graph.keys() {
                                                    contributions.insert(*vertex_id, 0.0);
                                                }

                                                for (src_id, rank) in ranks.iter() {
                                                    let edges_from_src =
                                                        graph.get(&src_id).unwrap();
                                                    let out_degree = edges_from_src.len();
                                                    let contribution: f64 =
                                                        *rank as f64 / out_degree as f64;
                                                    for dst_id in edges_from_src.into_iter() {
                                                        *contributions
                                                            .entry(*dst_id)
                                                            .or_insert(0f64) += contribution;
                                                    }
                                                }

                                                for (vertex_id, contribution) in
                                                    contributions.iter()
                                                {
                                                    session.give((*vertex_id, *contribution));
                                                }
                                            }
                                        }
                                    }
                                }

                                contribution_stash.retain(|_time, vals| !vals.is_empty());
                            }
                        },
                    );
                    let branches = rank_stream.branch_when(move |t| t.inner < 10);
                    branches.1.connect_loop(rank_iter_handler);
                    branches.0.leave()
                })
                .probe_with(&mut year_probe);
        });

        let dataset_dir_path = std::env::args().nth(1).unwrap();
        let year_lo: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        let year_hi: usize = std::env::args().nth(3).unwrap().parse().unwrap();

        if worker.index() == 0 {
            for year in year_lo..=year_hi {
                let vertex_file_path =
                    Path::new(&dataset_dir_path).join(format!("{}-verts.txt", year));
                let edges_file_path =
                    Path::new(&dataset_dir_path).join(format!("{}-edges.txt", year));

                graph_stream::from_vertex_list_file(&vertex_file_path, &mut vertex_stream_handler);
                vertex_stream_handler.advance_to(year - year_lo + 1);

                graph_stream::from_edge_list_file(&edges_file_path, &mut edge_stream_handler);
                edge_stream_handler.advance_to(year - year_lo + 1);

                println!("year: {}", year);
                while year_probe.less_than(edge_stream_handler.time()) {
                    worker.step();
                }
            }

            vertex_stream_handler.close();
            edge_stream_handler.close();
        }
    })
    .unwrap(); // asserts error-free execution;
}
