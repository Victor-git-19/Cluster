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
            f"{self.name}(dur={self.duration},"
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


def simulate_fifo(jobs, total_nodes):
    """Simulate FIFO"""
    cluster = Cluster(total_nodes)
    queue = deque(jobs)
    time = 0
    print("\n=== FIFO ===")
    while queue or cluster.running:
        if queue and cluster.can_run(queue[0]):
            job = queue.popleft()
            cluster.start_job(job)
            print(f"[t={time}] Start {job}")
        finished = cluster.tick()
        for f in finished:
            print(f"[t={time+1}] Finish {f}")
        time += 1
    print(f"[FIFO] Total time: {time}")
    return time


if __name__ == "__main__":
    jobs = [
        Job("A", 5, 2, 1),
        Job("B", 3, 3, 3),
        Job("C", 4, 1, 2),
        Job("D", 2, 2, 5),
    ]
    total_nodes = 4

    fifo_time = simulate_fifo([Job("A", 5, 2, 1),
                               Job("B", 3, 3, 3),
                               Job("C", 4, 1, 2),
                               Job("D", 2, 2, 5)
                               ], total_nodes)
