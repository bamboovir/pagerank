import heapq
import networkx as nx
import sys
from pathlib import Path
from pprint import pprint

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write(
            "Usage: pagerank.py <verticesPath> <edgesPath> <tolerance=1e-10> <dampingFactor=0.85> <topK=10>\n"
        )
        sys.exit(1)
    verticesPath = Path(sys.argv[1])
    edgesPath = Path(sys.argv[2])
    tolerance = float(sys.argv[3]) if len(sys.argv) > 3 else 1e-10
    dampingFactor = float(sys.argv[4]) if len(sys.argv) > 4 else 0.85
    topK = int(sys.argv[5]) if len(sys.argv) > 5 else 10

    graph = nx.DiGraph()

    with verticesPath.open(mode="r") as v, edgesPath.open(mode="r") as e:
        for line in v:
            vertex = int(line.strip())
            graph.add_node(vertex)
        for line in e:
            src, dst = line.split()
            src, dst = int(src), int(dst)
            graph.add_edge(src, dst)

    ranks = nx.pagerank(graph, alpha=dampingFactor, tol=tolerance)
    numOfNodes = graph.number_of_nodes()
    rankSum = 1.0
    correctionFactor = numOfNodes / rankSum
    ranks = {node: rank * correctionFactor for node, rank in ranks.items()}
    topKRanks = heapq.nlargest(topK, ranks.items(), key= lambda x: x[1])
    print(f"numOfNodes = {numOfNodes}")
    pprint(topKRanks)
