import networkx as nx
from collections import defaultdict
import random
import sys
from pathlib import Path
import heapq
from typing import Iterator
from pprint import pprint


class PageRankOnline:
    """
    R walks are sampled by using a random walk with a stopping probability of epsilon.

    walks: Record route of a random walk
    walksCounter: Frequency with which a given node is traversed in all random walks
    walksTable: A collection of route IDs for a given node

    The original CPP implementation:
    https://github.com/memgraph/mage/blob/main/cpp/pagerank_module/algorithm_online/pagerank.cpp
    """

    def __init__(self, R: int = 10, epsilon: float = 0.1) -> None:
        self.R = R
        self.epsilon = epsilon
        self.graph = nx.DiGraph()
        self.walks: list[list[int]] = []
        self.walksCounter: dict[int, int] = defaultdict(int)
        self.walksTable: dict[int, set[int]] = defaultdict(set)

    def pagerankTopK(self, topK: int) -> list[tuple[int, float]]:
        return heapq.nlargest(topK, self.pagerank(), key=lambda x: x[1])

    def pagerank(self) -> list[tuple[int, float]]:
        ranks = []
        numOfNodes = self.graph.number_of_nodes()

        for nodeID in self.graph.nodes:
            freq = self.walksCounter[nodeID]
            rank = freq / ((numOfNodes * self.R) / self.epsilon)
            ranks.append((nodeID, rank))

        return PageRankOnline.normalizeRank(ranks)

    @staticmethod
    def normalizeRank(ranks: list[tuple[int, float]]) -> list[tuple[int, float]]:
        numOfNodes = len(ranks)
        rankSum = sum(rank for _, rank in ranks)
        correctionFactor = numOfNodes / rankSum
        ranks = [(nodeID, rank * correctionFactor) for nodeID, rank in ranks]
        return ranks

    def createRoute(
        self, startID: int, walk: list[int], walkIndex: int, epsilon: float
    ):
        currID = startID
        while True:
            neighbors = [*self.graph.neighbors(currID)]
            if len(neighbors) == 0:
                break

            nextID = random.choice(neighbors)

            walk.append(nextID)
            self.walksTable[nextID].add(walkIndex)
            self.walksCounter[nextID] += 1

            if random.random() < epsilon:
                break

            currID = nextID

    def addEdge(self, srcID: int, dstID: int) -> None:
        self.graph.add_edge(srcID, dstID)
        walkIndexsCopy = [*self.walksTable[srcID]]

        for walkIndex in walkIndexsCopy:
            walk = self.walks[walkIndex]

            position = len(walk)
            for i in range(len(walk)):
                if walk[i] == srcID:
                    position = i
            position += 1
            erasePosition = position

            while position < len(walk):
                nodeID = position
                self.walksTable[nodeID].discard(walkIndex)
                self.walksCounter[nodeID] -= 1
                position += 1

            del walk[erasePosition:]
            self.createRoute(srcID, walk, walkIndex, self.epsilon / 2.0)

    def addVertex(self, nodeID: int) -> None:
        self.graph.add_node(nodeID)
        walkIndex = len(self.walks)
        for _ in range(self.R):
            walk = [nodeID]
            self.walksTable[nodeID].add(walkIndex)
            self.walksCounter[nodeID] += 1

            self.createRoute(nodeID, walk, walkIndex, self.epsilon)
            self.walks.append(walk)
            walkIndex += 1


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: pagerank_online.py <verticesPath> <edgesPath> <R> <epsilon> <topk>\n")
        sys.exit(1)

    verticesPath = Path(sys.argv[1])
    edgesPath = Path(sys.argv[2])
    R = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    epsilon = float(sys.argv[4]) if len(sys.argv) > 4 else 0.1
    topK = int(sys.argv[5]) if len(sys.argv) > 5 else 10

    pageRankOnline = PageRankOnline(R, epsilon)

    random.seed()

    with verticesPath.open(mode="r") as v, edgesPath.open(mode="r") as e:
        for line in v:
            vertex = int(line.strip())
            pageRankOnline.addVertex(vertex)
        for line in e:
            src, dst = line.split()
            src, dst = int(src), int(dst)
            pageRankOnline.addEdge(src, dst)

    topKRanks = pageRankOnline.pagerankTopK(topK)
    pprint(topKRanks)
