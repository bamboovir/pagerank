from collections import defaultdict
import itertools
from pathlib import Path
from datetime import datetime
import shutil


class CitHepPhPartitioner:
    """
    High-energy physics citation network dataset partitioner
    <https://snap.stanford.edu/data/cit-HepPh.html>
    """

    EDGES_FILENAME = "cit-HepPh.txt"
    NODES_DATES_FILENAME = "cit-HepPh-dates.txt"
    # EDGES_FILENAME = "hep-th-citations"
    # NODES_DATES_FILENAME = "hep-th-slacdates"
    BATCH_DIR = "batch"
    INCREMENTAL_DIR = "incremental"
    CROSS_LISTED_PAPER_ID_PERFIX = "11"

    def __init__(self, edgesPath: str, nodeDatesPath: str) -> None:
        edgesPath = Path(edgesPath)
        nodeDatesPath = Path(nodeDatesPath)
        self.srcToDst = CitHepPhPartitioner.parseEdges(edgesPath)
        self.nodeDates, self.nodeToDates = CitHepPhPartitioner.parseNodeDates(
            nodeDatesPath
        )
        self.years = sorted(set(date.year for _, date in self.nodeDates))

    @staticmethod
    def fromDefault(path: str = None) -> "CitHepPhPartitioner":
        if path == None:
            path = Path.cwd()
        else:
            path = Path(path)
        edgesPath = (path / CitHepPhPartitioner.EDGES_FILENAME).absolute().as_posix()
        nodeDatesPath = (
            (path / CitHepPhPartitioner.NODES_DATES_FILENAME).absolute().as_posix()
        )
        return CitHepPhPartitioner(edgesPath, nodeDatesPath)

    @staticmethod
    def parseEdges(path: Path) -> dict[str, list[str]]:
        srcToDst = defaultdict(list)
        nodes = set()
        with path.open(mode="r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or line == "":
                    continue
                src, dst = line.split()
                src, dst = int(src), int(dst)
                srcToDst[src].append(dst)
                nodes.add(src)
                nodes.add(dst)
        for node in nodes:
            if node not in srcToDst:
                srcToDst[node] = []
        return srcToDst

    @staticmethod
    def parseNodeDates(path: Path) -> list[tuple[str, datetime]]:
        nodeDates = []
        nodeToDates = dict()
        with path.open(mode="r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or line == "":
                    continue
                node, date = line.split()
                node = int(node)
                date = datetime.strptime(date, "%Y-%m-%d")
                nodeDates.append((node, date))
                nodeToDates[node] = date
        # sort by date
        nodeDates.sort(key=lambda x: x[1])
        return nodeDates, nodeToDates

    def batch(self, path: str = None) -> None:
        if path == None:
            path = Path.cwd() / CitHepPhPartitioner.BATCH_DIR
        else:
            path = Path(path)
        path: Path
        shutil.rmtree(path.absolute().as_posix())
        path.mkdir(parents=True, exist_ok=True)
        for year in self.years:
            currYearVertsPath = path / f"{year}-verts.txt"
            currYearEdgesPath = path / f"{year}-edges.txt"
            with currYearEdgesPath.open(mode="w") as e, currYearVertsPath.open(
                mode="w"
            ) as v:
                verts = set()
                for src, _ in itertools.takewhile(
                    lambda x: x[1].year < year + 1, self.nodeDates
                ):
                    if src not in self.srcToDst:
                        src = int(
                            str(src).removeprefix(
                                CitHepPhPartitioner.CROSS_LISTED_PAPER_ID_PERFIX
                            )
                        )
                        if src not in self.srcToDst:
                            continue
                    verts.add(src)
                    for dst in self.srcToDst[src]:
                        verts.add(dst)
                        e.write(f"{src} {dst}\n")
                for vert in verts:
                    v.write(f"{vert}\n")

    def incremental(self, path: str = None) -> None:
        if path == None:
            path = Path.cwd() / CitHepPhPartitioner.INCREMENTAL_DIR
        else:
            path = Path(path)
        path: Path
        shutil.rmtree(path.absolute().as_posix())
        path.mkdir(parents=True, exist_ok=True)
        for year, group in itertools.groupby(self.nodeDates, key=lambda x: x[1].year):
            currYearVertsPath = path / f"{year}-verts.txt"
            currYearEdgesPath = path / f"{year}-edges.txt"
            with currYearEdgesPath.open(mode="w") as e, currYearVertsPath.open(
                mode="w"
            ) as v:
                verts = set()
                for src, _ in group:
                    if src not in self.srcToDst:
                        src = int(
                            str(src).removeprefix(
                                CitHepPhPartitioner.CROSS_LISTED_PAPER_ID_PERFIX
                            )
                        )
                        if src not in self.srcToDst:
                            continue
                    verts.add(src)
                    for dst in self.srcToDst[src]:
                        verts.add(dst)
                        e.write(f"{src} {dst}\n")

                for vert in verts:
                    v.write(f"{vert}\n")


if __name__ == "__main__":
    partitioner = CitHepPhPartitioner.fromDefault()
    partitioner.incremental()
    partitioner.batch()
