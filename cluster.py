import heapq
from collections import deque


class Job:
    """Job"""
    def __init__(self, name, duration, nodes_required, priority=0):
        self.name = name
        self.duration = duration
        self.nodes_required = nodes_required
        self.priority = priority
        self.remaining = duration

    def __repr__(self):
        return f"{self.name}(dur={self.duration},nodes={self.nodes_required},pri={self.priority})"


class Cluster:
    """Cluster"""
    def __init__(self, total_nodes):
        self.total_nodes = total_nodes
        self.available = total_nodes
        self.running = []

    def can_run(self, job):
        """Fit"""
        return self.available >= job.nodes_required

    def start(self, job):
        """Start"""
        self.available -= job.nodes_required
        self.running.append(job)

    def tick(self):
        """Tick"""
        finished = []
        for job in list(self.running):
            job.remaining -= 1
            if job.remaining <= 0:
                finished.append(job)
                self.running.remove(job)
                self.available += job.nodes_required
        return finished


def simulate_fifo(jobs, total_nodes):
    """FIFO"""
    queue = deque(jobs)
    cluster = Cluster(total_nodes)
    time = 0
    print("\n=== FIFO ===")

    while queue or cluster.running:
        for job in list(queue):
            if cluster.can_run(job):
                cluster.start(job)
                print(f"[t={time}] Start {job}")
                queue.remove(job)
            else:
                break

        finished = cluster.tick()
        for job in finished:
            print(f"[t={time+1}] Finish {job}")

        time += 1

    print(f"[FIFO] Total time: {time}")
    return time


def simulate_priority(jobs, total_nodes):
    """Priority"""
    cluster = Cluster(total_nodes)
    waiting = [(-job.priority, idx, job) for idx, job in enumerate(jobs)]
    heapq.heapify(waiting)
    time = 0
    print("\n=== PRIORITY ===")

    while waiting or cluster.running:
        block = []
        while waiting:
            priority, idx, job = heapq.heappop(waiting)
            if cluster.can_run(job):
                cluster.start(job)
                print(f"[t={time}] Start {job}")
            else:
                block.append((priority, idx, job))
        for item in block:
            heapq.heappush(waiting, item)

        finished = cluster.tick()
        for job in finished:
            print(f"[t={time+1}] Finish {job}")
        time += 1

    print(f"[PRIORITY] Total time: {time}")
    return time


def simulate_backfill(jobs, total_nodes):
    """Backfill"""
    queue = deque(jobs)
    cluster = Cluster(total_nodes)
    time = 0
    print("\n=== BACKFILL ===")

    while queue or cluster.running:
        if queue and cluster.can_run(queue[0]):
            job = queue.popleft()
            cluster.start(job)
            print(f"[t={time}] Start {job}")
        elif queue and cluster.available > 0:
            soonest_finish = min((job.remaining for job in cluster.running),
                                 default=0)
            for job in list(queue)[1:]:
                if cluster.can_run(job) and job.duration <= soonest_finish:
                    cluster.start(job)
                    print(f"[t={time}] Start {job} (backfill)")
                    queue.remove(job)

        finished = cluster.tick()
        for job in finished:
            print(f"[t={time+1}] Finish {job}")
        time += 1

    print(f"[BACKFILL] Total time: {time}")
    return time


if __name__ == "__main__":
    base_jobs = [
        Job("A", 5, 2, 1),
        Job("B", 3, 3, 3),
        Job("C", 4, 1, 2),
        Job("D", 2, 2, 5),
    ]

    total_nodes = 5

    def clone_jobs():
        """Clone"""
        return [Job(job.name, job.duration, job.nodes_required, job.priority) for job in base_jobs]

    fifo_time = simulate_fifo(clone_jobs(), total_nodes)
    prio_time = simulate_priority(clone_jobs(), total_nodes)
    backfill_time = simulate_backfill(clone_jobs(), total_nodes)

    print("\n=== SUMMARY ===")
    print(f"Total nodes: {total_nodes}")
    print(f"Number of jobs: {len(base_jobs)}")
    print(f"FIFO:{fifo_time} PRIORITY:{prio_time} BACKFILL:{backfill_time}")
