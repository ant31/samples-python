import random
from typing import List
from uuid import UUID

from temporalio import activity

from activity_sticky_queues import tasks
from temporalloop.config import Config, WorkerConfig
from temporalloop.main import run


@activity.defn(name="get_available_task_queue")
async def select_task_queue_random() -> str:
    """Randomly assign the job to a queue"""
    return random.choice(task_queues)

def main():
    # Uncomment the line below to see logging
    # logging.basicConfig(level=logging.INFO)

    # Comment line to see non-deterministic functionality
    random.seed(667)

    # Create random task queues and build task queue selection function
    task_queues: List[str] = [
        f"activity_sticky_queue-host-{UUID(int=random.getrandbits(128))}"
        for _ in range(5)
    ]

    # worker to distribute the workflows
    distributor_worker = WorkerConfig(name="base-queue-distributor",
        queue="activity_sticky_queue-distribution-queue",
        workflows=[tasks.FileProcessing],
        activities=[select_task_queue_random])

    # workers to process queues
    processing_workers = []
    for queue_id in task_queues:
        processing_workers.append(
            WorkerConfig(name="base-queue-distributor",
                         queue=queue_id,
                         activities=[
                             tasks.download_file_to_worker_filesystem,
                             tasks.work_on_file_in_worker_filesystem,
                             tasks.clean_up_file_from_worker_filesystem,
                         ]))

    run(Config(
        host="localhost:7233",
        workers=[distributor_worker] + processing_workers))




if __name__ == "__main__":
    main()
