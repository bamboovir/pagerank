# NetworkX pagerank

This solution aims to verify the correctness of our custom implemented PageRank algorithm.
This directory also provides the python implementation of [pagerank_online](http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf).

Set the current directory

```bash
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/networkx"
```

Set up Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python --version
# Python 3.10.8
pip install -r requirements.txt
```

```bash
# Usage: pagerank.py <verticesPath> <edgesPath> <tolerance=1e-10> <dampingFactor=0.85> <topK=10>
python pagerank.py \
  "${ROOT}/dataset/batch/2002-verts.txt" \
  "${ROOT}/dataset/batch/2002-edges.txt" \
  1e-10 \
  0.85 \
  10
```

```bash
# Usage: pagerank_online.py <verticesPath> <edgesPath> <R> <epsilon> <topk>
python pagerank_online.py \
  "${ROOT}/dataset/batch/2002-verts.txt" \
  "${ROOT}/dataset/batch/2002-edges.txt" \
  10 \
  0.1 \
  10
```
