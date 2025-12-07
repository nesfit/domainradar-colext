from sortedcontainers import SortedDict

Partition = int
Offset = int
ToProcessEntry = tuple[Partition, Offset, bytes, bytes]
"""A tuple of partition, offset, key, and value."""
SimpleMessage = tuple[str, bytes, bytes]
"""A tuple of topic, key, and value."""
ProcessedEntry = tuple[Partition, Offset, list[SimpleMessage]]
ROB = SortedDict[Offset, float | ProcessedEntry]
ROBContainer = dict[Partition, ROB]
