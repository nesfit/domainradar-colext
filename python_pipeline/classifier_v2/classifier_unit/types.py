from sortedcontainers import SortedDict


Partition = int
Offset = int
ToProcessEntry = tuple[Partition, Offset, bytes]
SimpleMessage = tuple[bytes, bytes]
ProcessedEntry = tuple[Partition, Offset, list[SimpleMessage]]
ROB = SortedDict[Offset, float | ProcessedEntry]
ROBContainer = dict[Partition, ROB]