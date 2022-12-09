use clap::Parser;
use pagerank::graph_stream;
use pagerank::rank_utils;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::ResultStream;
use timely::dataflow::operators::{Concat, ConnectLoop, Enter, Leave, LoopVariable, Map, Probe};
use timely::dataflow::Scope;
use timely::dataflow::{InputHandle, ProbeHandle};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct PageRankArgs {
    #[arg(long)]
    graph_data_directory: String,
    #[arg(long, default_value_t = 1e-10)]
    tolerance: f64,
    #[arg(long, default_value_t = 0.85)]
    damping_factor: f64,
    #[arg(long, default_value_t = 10)]
    topk: usize,
    #[arg(long, default_value_t = 1992)]
    year_lo: usize,
    #[arg(long, default_value_t = 2002)]
    year_hi: usize,
    timely_args: Vec<String>,
}

fn main() {
    let pagerank_args = PageRankArgs::parse();

    timely::execute_from_args(pagerank_args.timely_args.into_iter(), move |worker| {
        let dataset_dir_path = pagerank_args.graph_data_directory.clone();
        let year_lo = pagerank_args.year_lo;
        let year_hi = pagerank_args.year_hi;
        let topk = pagerank_args.topk;
        let tolerance = pagerank_args.tolerance;
        let damping_factor = pagerank_args.damping_factor;
        let random_reset_probability = 1.0f64 - pagerank_args.damping_factor;

        let mut vertex_stream_handler = InputHandle::new();
        let mut edge_stream_handler = InputHandle::new();

        let mut year_probe = ProbeHandle::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let vertex_stream = vertex_stream_handler
                .to_stream(scope)
                .map(|vertex_id| (vertex_id, 0u64));

            let edge_stream = edge_stream_handler.to_stream(scope).concat(&vertex_stream);

            let rank_stream = scope.iterative::<usize, _, _>(|subscope| {
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

                            let frontiers = &[edge_input.frontier(), contribution_input.frontier()];

                            for (time, edge_changes) in edge_stash.iter_mut() {
                                if frontiers.iter().all(|f| !f.less_equal(time)) {
                                    eprintln!(
                                        "edge_stream: outer time = {} inner time = {}",
                                        time.time().outer,
                                        time.time().inner
                                    );
                                    let mut session = output.session(time);
                                    for (src_id, dst_id) in edge_changes.drain(..) {
                                        if dst_id == 0 {
                                            let vertex_id = src_id;
                                            graph.entry(vertex_id).or_insert(Vec::new());
                                        } else {
                                            graph.entry(src_id).or_insert(Vec::new()).push(dst_id);
                                        }
                                    }

                                    for vertex_id in graph.keys() {
                                        session.give((*vertex_id, 1.0f64, false));
                                        *ranks.entry(*vertex_id).or_insert(1.0f64) = 1.0f64;
                                    }
                                }
                            }

                            edge_stash.retain(|_time, vals| !vals.is_empty());

                            for (time, contribution_changes) in contribution_stash.iter_mut() {
                                if frontiers.iter().all(|f| !f.less_equal(time)) {
                                    eprintln!(
                                        "contribution_stream: outer time = {} inner time = {}",
                                        time.time().outer,
                                        time.time().inner
                                    );
                                    let mut session = output.session(time);

                                    let mut next_ranks: HashMap<u64, f64> = HashMap::new();

                                    for (vertex_id, contribution) in contribution_changes.drain(..)
                                    {
                                          *next_ranks.entry(vertex_id).or_insert(0.0f64) +=
                                            contribution;
                                    }

                                    for (_vertex_id, contribution) in next_ranks.iter_mut() {
                                        *contribution = random_reset_probability + damping_factor * *contribution;
                                    }

                                    let mut convergenced = true;

                                    for (vertex_id, rank) in ranks.iter_mut() {
                                        let next_rank = next_ranks[vertex_id];
                                        if (next_rank - *rank).abs() >= tolerance {
                                            convergenced = false;
                                        }
                                        *rank = next_rank;
                                    }

                                    if time.inner == 1usize {
                                        convergenced = false;
                                    }

                                    let mut contributions: HashMap<u64, f64> =
                                        HashMap::new();

                                    for vertex_id in graph.keys() {
                                        contributions.insert(*vertex_id, 0.0f64);
                                    }

                                    for (src_id, rank) in ranks.iter() {
                                        let edges_from_src = graph.get(&src_id).unwrap();
                                        let out_degree = edges_from_src.len();
                                        let contribution: f64 =
                                            *rank as f64 / out_degree as f64;
                                        for dst_id in edges_from_src.into_iter() {
                                            *contributions
                                                .entry(*dst_id)
                                                .or_insert(0.0f64) += contribution;
                                        }
                                    }

                                    for (vertex_id, contribution) in contributions.iter() {
                                        session.give((*vertex_id, *contribution, convergenced));
                                    }
                                }
                            }

                            contribution_stash.retain(|_time, vals| !vals.is_empty());
                        }
                    },
                );

                let rank_iter_diverter_stream = rank_stream.unary_frontier(
                    Exchange::new(|_| 0),
                    "ReduceRanksUntilConvergenced",
                    |_capability, _info| {
                        let mut contribution_stash = HashMap::new();

                        move |contribution_input, output| {
                            while let Some((time, data)) = contribution_input.next() {
                                contribution_stash
                                    .entry(time.retain())
                                    .or_insert(Vec::new())
                                    .extend(data.replace(Vec::new()));
                            }

                            for (time, contribution_changes) in contribution_stash.iter_mut() {
                                let mut session = output.session(time);

                                if !contribution_input.frontier().less_equal(time) {
                                    let convergenced = contribution_changes.iter().all(|(_, _, convergenced)| *convergenced);

                                    if convergenced {
                                        eprintln!(
                                            "convergenced_rank_stream: outer time = {} inner time = {}",
                                            time.time().outer,
                                            time.time().inner
                                        );
                                        println!(
                                            "iteration_times_to_convergence = {}",
                                            (time.time().inner as i64) - 1
                                        );
                                        let mut ranks: HashMap<u64, f64> = HashMap::new();

                                        for (vertex_id, contribution, _) in contribution_changes.drain(..) {
                                            *ranks.entry(vertex_id).or_insert(0.0f64) +=
                                                    contribution;
                                        }

                                        for (_vertex_id, contribution) in ranks.iter_mut() {
                                            *contribution = random_reset_probability + damping_factor * *contribution;
                                        }

                                        for (vertex_id, rank) in ranks.iter() {
                                            session.give(Ok((*vertex_id, *rank)));
                                        }

                                    } else {
                                        for (vertex_id, rank, _) in contribution_changes.drain(..) {
                                            session.give(Err((vertex_id, rank)));
                                        }
                                    }
                                }
                            }

                            contribution_stash.retain(|_time, vals| !vals.is_empty());
                        }
                    },
                );

                let unconverged_rank_iter_stream =  rank_iter_diverter_stream.err();
                unconverged_rank_iter_stream.connect_loop(rank_iter_handler);
                let converged_rank_iter_stream = rank_iter_diverter_stream.ok();
                converged_rank_iter_stream.leave()
            });

            let topk_rank_stream = rank_stream
                .unary_frontier(
                    Exchange::new(|_| 0),
                    "TopKRank", 
                    |_capability, _info| {
                    let mut ranks: HashMap<u64, f64> = HashMap::new();
                    let mut rank_stash = HashMap::new();

                    move |rank_input, output| {
                        while let Some((time, data)) = rank_input.next() {
                            rank_stash
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .extend(data.replace(Vec::new()));
                        }

                        let frontier = rank_input.frontier();

                        for (time, rank_changes) in rank_stash.iter_mut() {
                            let mut session = output.session(time);
                            if !frontier.less_equal(time) {
                                for (vertex_id, rank) in rank_changes.drain(..) {
                                    ranks.insert(vertex_id, rank);
                                }

                                let mut normalized_ranks: HashMap<u64, f64> =
                                    HashMap::new();
                                rank_utils::normalize_ranks(&ranks, &mut normalized_ranks);
                                let mut topked_ranks = vec![];
                                rank_utils::topk_ranks(
                                    &normalized_ranks,
                                    &mut topked_ranks,
                                    topk,
                                );
                                session.give(topked_ranks);
                            }
                        }

                        rank_stash.retain(|_time, vals| !vals.is_empty());
                    }
                });

            topk_rank_stream
            .inspect(|topked_ranks| rank_utils::display_ranks(topked_ranks))
            .probe_with(&mut year_probe);
        });

        if worker.index() == 0 {
            let start_instant = Instant::now();
            for year in year_lo..=year_hi {
                println!("year: {}", year);
                let vertex_file_path =
                    Path::new(&dataset_dir_path).join(format!("{}-verts.txt", year));
                let edges_file_path =
                    Path::new(&dataset_dir_path).join(format!("{}-edges.txt", year));

                graph_stream::from_vertex_list_file(&vertex_file_path, &mut vertex_stream_handler);
                vertex_stream_handler.advance_to(year - year_lo + 1);

                graph_stream::from_edge_list_file(&edges_file_path, &mut edge_stream_handler);
                edge_stream_handler.advance_to(year - year_lo + 1);

                while year_probe.less_than(edge_stream_handler.time()) {
                    worker.step();
                }
                let cumulative_elapsed_time = start_instant.elapsed();
                eprintln!("cumulative_elapsed_time: {}", cumulative_elapsed_time.as_millis());
            }

            vertex_stream_handler.close();
            edge_stream_handler.close();
        }
    })
    .unwrap(); // asserts error-free execution;
}
