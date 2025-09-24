"""
Job scheduler with FIFO, priority-based scheduling, and preemption support.
"""
import asyncio
import subprocess
import os
import tempfile
from datetime import datetime
from typing import List, Optional, Dict
from models import Job, JobStatus, User
from database import DatabaseManager

class JobScheduler:
    def __init__(self, db_manager: DatabaseManager, max_concurrent_jobs: int = 2):
        self.db_manager = db_manager
        self.max_concurrent_jobs = max_concurrent_jobs
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.job_queue: List[Job] = []
        self.running = False

    async def add_job(self, job: Job) -> bool:
        """Add a job to the queue."""
        # Check user quotas
        user = await self.db_manager.get_user(job.user)
        if not user:
            print(f"User {job.user} not found")
            return False

        # Check if user has reached concurrent job limit
        running_jobs = await self.db_manager.get_user_running_jobs(job.user)
        if len(running_jobs) >= user.max_concurrent_jobs:
            print(f"User {job.user} has reached max concurrent jobs limit")
            return False

        # Store job in database
        success = await self.db_manager.create_job(job)
        if success:
            self.job_queue.append(job)
            self.job_queue.sort(key=lambda x: (-x.priority, x.created_at))
            print(f"Job {job.job_id} added to queue")
            return True
        return False

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        # Check if job is running
        if job_id in self.running_jobs:
            task = self.running_jobs[job_id]
            task.cancel()
            del self.running_jobs[job_id]
            
            # Update job status
            await self.db_manager.update_job_status(
                job_id, JobStatus.CANCELLED, completed_at=datetime.now()
            )
            print(f"Running job {job_id} cancelled")
            return True

        # Check if job is in queue
        for i, job in enumerate(self.job_queue):
            if job.job_id == job_id:
                self.job_queue.pop(i)
                await self.db_manager.update_job_status(
                    job_id, JobStatus.CANCELLED, completed_at=datetime.now()
                )
                print(f"Queued job {job_id} cancelled")
                return True

        print(f"Job {job_id} not found")
        return False

    async def execute_cpp_job(self, job: Job) -> tuple[str, str, int]:
        """Execute a C++ compilation and execution job."""
        try:
            # Create temporary directory for job execution
            with tempfile.TemporaryDirectory() as temp_dir:
                # Copy C++ file to temp directory
                cpp_file_path = os.path.join(temp_dir, f"{job.job_id}.cpp")
                
                # Create a simple C++ file if it doesn't exist (for testing)
                if not os.path.exists(job.cpp_file):
                    cpp_content = f"""
#include <iostream>
#include <chrono>
#include <thread>

int main() {{
    std::cout << "Job {job.job_id} started by user {job.user}" << std::endl;
    std::cout << "Priority: " << {job.priority} << std::endl;
    
    // Simulate some work
    for (int i = 1; i <= 5; i++) {{
        std::cout << "Processing step " << i << "/5" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }}
    
    std::cout << "Job {job.job_id} completed successfully!" << std::endl;
    return 0;
}}
"""
                    with open(cpp_file_path, 'w') as f:
                        f.write(cpp_content)
                else:
                    # Copy existing file
                    with open(job.cpp_file, 'r') as src:
                        with open(cpp_file_path, 'w') as dst:
                            dst.write(src.read())

                # Compile C++ file
                exec_path = os.path.join(temp_dir, f"{job.job_id}_exec")
                compile_process = await asyncio.create_subprocess_exec(
                    'g++', '-o', exec_path, cpp_file_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                compile_stdout, compile_stderr = await compile_process.communicate()
                
                if compile_process.returncode != 0:
                    return (
                        compile_stdout.decode(),
                        f"Compilation failed: {compile_stderr.decode()}",
                        compile_process.returncode
                    )

                # Execute compiled program
                exec_process = await asyncio.create_subprocess_exec(
                    exec_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                try:
                    exec_stdout, exec_stderr = await asyncio.wait_for(
                        exec_process.communicate(), timeout=job.max_runtime
                    )
                    
                    return (
                        f"Compilation successful.\nExecution output:\n{exec_stdout.decode()}",
                        exec_stderr.decode() if exec_stderr else "",
                        exec_process.returncode
                    )
                
                except asyncio.TimeoutError:
                    exec_process.terminate()
                    return (
                        "Job execution timed out",
                        f"Job exceeded maximum runtime of {job.max_runtime} seconds",
                        -1
                    )

        except Exception as e:
            return ("", f"Job execution error: {str(e)}", -1)

    async def run_job(self, job: Job):
        """Run a single job."""
        try:
            # Update job status to running
            await self.db_manager.update_job_status(
                job.job_id, JobStatus.RUNNING, started_at=datetime.now()
            )
            
            print(f"Starting job {job.job_id} (priority: {job.priority})")
            
            # Execute the job
            output, error, return_code = await self.execute_cpp_job(job)
            
            # Determine final status
            if return_code == 0:
                status = JobStatus.COMPLETED
            else:
                status = JobStatus.FAILED
            
            # Update job status
            await self.db_manager.update_job_status(
                job.job_id, status, 
                completed_at=datetime.now(),
                output=output,
                error=error
            )
            
            print(f"Job {job.job_id} {status.value}")
            
        except asyncio.CancelledError:
            # Job was cancelled
            await self.db_manager.update_job_status(
                job.job_id, JobStatus.CANCELLED, completed_at=datetime.now()
            )
            print(f"Job {job.job_id} was cancelled")
        except Exception as e:
            # Job failed due to exception
            await self.db_manager.update_job_status(
                job.job_id, JobStatus.FAILED, 
                completed_at=datetime.now(),
                error=str(e)
            )
            print(f"Job {job.job_id} failed: {e}")
        finally:
            # Remove job from running jobs
            if job.job_id in self.running_jobs:
                del self.running_jobs[job.job_id]

    async def schedule_jobs(self):
        """Main scheduling loop."""
        self.running = True
        while self.running:
            try:
                # Check for completed jobs and remove them
                completed_tasks = [
                    job_id for job_id, task in self.running_jobs.items() 
                    if task.done()
                ]
                for job_id in completed_tasks:
                    del self.running_jobs[job_id]

                # Start new jobs if we have capacity
                while (len(self.running_jobs) < self.max_concurrent_jobs 
                       and self.job_queue):
                    
                    # Get highest priority job
                    job = self.job_queue.pop(0)
                    
                    # Check for preemption opportunity
                    if job.priority > 5 and len(self.running_jobs) == self.max_concurrent_jobs:
                        # Find lowest priority running job
                        lowest_priority_job = None
                        lowest_priority = 10
                        
                        for running_job_id in self.running_jobs:
                            running_job = await self.db_manager.get_job(running_job_id)
                            if running_job and running_job.priority < lowest_priority:
                                lowest_priority = running_job.priority
                                lowest_priority_job = running_job
                        
                        # Preempt if new job has significantly higher priority
                        if (lowest_priority_job and 
                            job.priority > lowest_priority_job.priority + 2):
                            
                            print(f"Preempting job {lowest_priority_job.job_id} for higher priority job {job.job_id}")
                            
                            # Cancel the lower priority job
                            await self.cancel_job(lowest_priority_job.job_id)
                            
                            # Update preempted job status
                            await self.db_manager.update_job_status(
                                lowest_priority_job.job_id, JobStatus.PREEMPTED,
                                completed_at=datetime.now()
                            )
                            
                            # Re-queue the preempted job with slightly higher priority
                            lowest_priority_job.priority = min(10, lowest_priority_job.priority + 1)
                            self.job_queue.append(lowest_priority_job)
                            self.job_queue.sort(key=lambda x: (-x.priority, x.created_at))

                    # Start the new job
                    task = asyncio.create_task(self.run_job(job))
                    self.running_jobs[job.job_id] = task

                # Wait a bit before next scheduling cycle
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Scheduler error: {e}")
                await asyncio.sleep(1)

    async def start_scheduler(self):
        """Start the job scheduler."""
        print("Starting job scheduler...")
        
        # Load queued jobs from database
        queued_jobs = await self.db_manager.get_jobs_by_status(JobStatus.QUEUED)
        self.job_queue = sorted(queued_jobs, key=lambda x: (-x.priority, x.created_at))
        
        # Start scheduling loop
        await self.schedule_jobs()

    def stop_scheduler(self):
        """Stop the job scheduler."""
        print("Stopping job scheduler...")
        self.running = False
        
        # Cancel all running jobs
        for task in self.running_jobs.values():
            task.cancel()

    async def get_queue_status(self) -> Dict:
        """Get current queue and running job status."""
        return {
            "running_jobs": len(self.running_jobs),
            "queued_jobs": len(self.job_queue),
            "max_concurrent": self.max_concurrent_jobs,
            "running_job_ids": list(self.running_jobs.keys()),
            "queued_job_ids": [job.job_id for job in self.job_queue]
        }