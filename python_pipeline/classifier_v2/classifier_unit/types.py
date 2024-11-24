from sortedcontainers import SortedDict


Partition = int
Offset = int
ToProcessEntry = tuple[Partition, Offset]
ROB = SortedDict[Offset, float | None]
ROBContainer = dict[Partition, ROB]