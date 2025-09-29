from collections import deque
import heapq


# Job`s class
class Job:
    """Job Class"""
    def __init__(self, name, duration, nodes_required, priority=0):
        self.name = name
        self.duration = duration
        self.nodes_required = nodes_required
        self.priority = priority
        self.remaining = duration

    def __repr__(self):
        return (
            f"{self.name}(dur={self.duration},\n"
            f"nodes={self.nodes_required}, pri={self.priority})")
