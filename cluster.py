from collections import deque
import heapq


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


class Cluster:
    """Cluster Class"""
    def __init__(self, total_nodes):
        self.total_nodes = total_nodes
        self.available_nodes = total_nodes
        self.running = []

    def can_run(self, job):
        return self.available_nodes >= job.nodes_required

    def start_job(self, job):
        self.available_nodes -= job.nodes_required
        self.running.append(job)

    def tick(self):
        finished = []
        for job in list(self.running):
            job.remaining -= 1
            if job.remaining <= 0:
                finished.append(job)
                self.running.remove(job)
                self.available_nodes += job.nodes_required
        return finished
