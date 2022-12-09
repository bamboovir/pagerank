# Timely Dataflow incremental pagerank

Set the current directory

```bash
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/timely-dataflow/pagerank"
```

Build

```bash
cargo build --release
```

Usage

```bash
cargo run --release --bin pagerank -- --help
```

```bash
Usage: pagerank [OPTIONS] --graph-data-directory <GRAPH_DATA_DIRECTORY> [TIMELY_ARGS]...

Arguments:
  [TIMELY_ARGS]...  

Options:
      --graph-data-directory <GRAPH_DATA_DIRECTORY>  
      --tolerance <TOLERANCE>                        [default: 0.0000000001]
      --damping-factor <DAMPING_FACTOR>              [default: 0.85]
      --topk <TOPK>                                  [default: 10]
      --year-lo <YEAR_LO>                            [default: 1992]
      --year-hi <YEAR_HI>                            [default: 2002]
  -h, --help                                         Print help information
  -V, --version                                      Print version information
```

```bash
cargo run --release --bin pagerank -- \
  --graph-data-directory "${ROOT}/dataset/incremental" \
  --tolerance 1e-10 \
  --damping-factor 0.85 \
  --topk 10 \
  --year-lo 1992 \
  --year-hi 2002 \
  2> /dev/null

cargo run --release --bin pagerank -- \
  --graph-data-directory "${ROOT}/dataset/incremental" \
  --tolerance 1e-10 \
  --damping-factor 0.85 \
  --topk 10 \
  --year-lo 1992 \
  --year-hi 2002 \
  -- \
  -w 2 \
  2> /dev/null

cargo run --release --bin pagerank -- \
  --graph-data-directory "${ROOT}/dataset/incremental" \
  --tolerance 1e-10 \
  --damping-factor 0.85 \
  --topk 10 \
  --year-lo 1992 \
  --year-hi 2002 \
  -- \
  -w 4 \
  2> /dev/null
```
