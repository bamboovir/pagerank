# DataSet

Set the current directory

```bash
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/dataset"
```

Download dataset

```bash
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh.txt.gz" | gunzip -d > cit-HepPh.txt
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh-dates.txt.gz" | gunzip -d > cit-HepPh-dates.txt
# OR https://www.cs.cornell.edu/projects/kddcup/datasets.html
```

Set up Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python --version
# Python 3.10.8
```

Run the partition script.
Two directories will be generated in dataset directory: [`batch`, `incremental`]

This data partitioning script performs data cleaning of the High-energy physics citation network dataset.
Specifically, some nodes in the edge file do not have a date, and vice versa, nodes with a date are not necessarily in the edge file.
For this case, we took the intersection of these two datasets, which means that a node must reference or be referenced by other nodes in the edge file, and this node must have an exact date.

```bash
python partition.py
```
