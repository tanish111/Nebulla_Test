"""
Database layer for the CPU orchestrator daemon.
"""
import asyncio
from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, update, delete
from models import Base, JobDB, UserDB, MetricsDB, Job, User, GPUMetrics, JobStatus

class DatabaseManager:
    def __init__(self, database_url: str = "sqlite+aiosqlite:///cpu_orchestrator.db"):
        self.engine = create_async_engine(database_url, echo=False)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def init_db(self):
        """Initialize database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
        # Create default user if not exists
        await self.create_user_if_not_exists("admin", 10, 8, 16.0)

    async def create_user_if_not_exists(self, username: str, max_jobs: int = 3, 
                                      max_cores: int = 4, max_memory: float = 8.0):
        """Create a user if they don't exist."""
        async with self.async_session() as session:
            result = await session.execute(
                select(UserDB).where(UserDB.username == username)
            )
            if not result.scalar_one_or_none():
                user = UserDB(
                    username=username,
                    max_concurrent_jobs=max_jobs,
                    max_cpu_cores=max_cores,
                    max_memory_gb=max_memory
                )
                session.add(user)
                await session.commit()

    async def create_job(self, job: Job) -> bool:
        """Create a new job in the database."""
        try:
            async with self.async_session() as session:
                job_db = JobDB(
                    job_id=job.job_id,
                    cpp_file=job.cpp_file,
                    user=job.user,
                    priority=job.priority,
                    status=job.status.value,
                    created_at=job.created_at,
                    cpu_cores_allocated=job.cpu_cores_allocated,
                    memory_allocated=job.memory_allocated,
                    max_runtime=job.max_runtime
                )
                session.add(job_db)
                await session.commit()
                return True
        except Exception as e:
            print(f"Error creating job: {e}")
            return False

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        async with self.async_session() as session:
            result = await session.execute(
                select(JobDB).where(JobDB.job_id == job_id)
            )
            job_db = result.scalar_one_or_none()
            if job_db:
                return Job(
                    job_id=job_db.job_id,
                    cpp_file=job_db.cpp_file,
                    user=job_db.user,
                    priority=job_db.priority,
                    status=JobStatus(job_db.status),
                    created_at=job_db.created_at,
                    started_at=job_db.started_at,
                    completed_at=job_db.completed_at,
                    output=job_db.output,
                    error=job_db.error,
                    cpu_cores_allocated=job_db.cpu_cores_allocated,
                    memory_allocated=job_db.memory_allocated,
                    max_runtime=job_db.max_runtime
                )
            return None

    async def get_all_jobs(self) -> List[Job]:
        """Get all jobs."""
        async with self.async_session() as session:
            result = await session.execute(select(JobDB))
            jobs = []
            for job_db in result.scalars():
                jobs.append(Job(
                    job_id=job_db.job_id,
                    cpp_file=job_db.cpp_file,
                    user=job_db.user,
                    priority=job_db.priority,
                    status=JobStatus(job_db.status),
                    created_at=job_db.created_at,
                    started_at=job_db.started_at,
                    completed_at=job_db.completed_at,
                    output=job_db.output,
                    error=job_db.error,
                    cpu_cores_allocated=job_db.cpu_cores_allocated,
                    memory_allocated=job_db.memory_allocated,
                    max_runtime=job_db.max_runtime
                ))
            return jobs

    async def update_job_status(self, job_id: str, status: JobStatus, 
                               started_at: Optional[datetime] = None,
                               completed_at: Optional[datetime] = None,
                               output: Optional[str] = None,
                               error: Optional[str] = None) -> bool:
        """Update job status and related fields."""
        try:
            async with self.async_session() as session:
                update_data = {"status": status.value}
                if started_at:
                    update_data["started_at"] = started_at
                if completed_at:
                    update_data["completed_at"] = completed_at
                if output:
                    update_data["output"] = output
                if error:
                    update_data["error"] = error
                
                await session.execute(
                    update(JobDB).where(JobDB.job_id == job_id).values(**update_data)
                )
                await session.commit()
                return True
        except Exception as e:
            print(f"Error updating job status: {e}")
            return False

    async def get_user(self, username: str) -> Optional[User]:
        """Get user by username."""
        async with self.async_session() as session:
            result = await session.execute(
                select(UserDB).where(UserDB.username == username)
            )
            user_db = result.scalar_one_or_none()
            if user_db:
                return User(
                    username=user_db.username,
                    max_concurrent_jobs=user_db.max_concurrent_jobs,
                    max_cpu_cores=user_db.max_cpu_cores,
                    max_memory_gb=user_db.max_memory_gb,
                    created_at=user_db.created_at
                )
            return None

    async def get_all_users(self) -> List[User]:
        """Get all users."""
        async with self.async_session() as session:
            result = await session.execute(select(UserDB))
            users = []
            for user_db in result.scalars():
                users.append(User(
                    username=user_db.username,
                    max_concurrent_jobs=user_db.max_concurrent_jobs,
                    max_cpu_cores=user_db.max_cpu_cores,
                    max_memory_gb=user_db.max_memory_gb,
                    created_at=user_db.created_at
                ))
            return users

    async def create_user(self, user: User) -> bool:
        """Create a new user."""
        try:
            async with self.async_session() as session:
                user_db = UserDB(
                    username=user.username,
                    max_concurrent_jobs=user.max_concurrent_jobs,
                    max_cpu_cores=user.max_cpu_cores,
                    max_memory_gb=user.max_memory_gb
                )
                session.add(user_db)
                await session.commit()
                return True
        except Exception as e:
            print(f"Error creating user: {e}")
            return False

    async def store_metrics(self, metrics: GPUMetrics) -> bool:
        """Store GPU metrics."""
        try:
            async with self.async_session() as session:
                metrics_db = MetricsDB(
                    gpu_utilization=metrics.gpu_utilization,
                    memory_used=metrics.memory_used,
                    memory_total=metrics.memory_total,
                    temperature=metrics.temperature,
                    timestamp=metrics.timestamp
                )
                session.add(metrics_db)
                await session.commit()
                return True
        except Exception as e:
            print(f"Error storing metrics: {e}")
            return False

    async def get_jobs_by_status(self, status: JobStatus) -> List[Job]:
        """Get jobs by status."""
        async with self.async_session() as session:
            result = await session.execute(
                select(JobDB).where(JobDB.status == status.value)
            )
            jobs = []
            for job_db in result.scalars():
                jobs.append(Job(
                    job_id=job_db.job_id,
                    cpp_file=job_db.cpp_file,
                    user=job_db.user,
                    priority=job_db.priority,
                    status=JobStatus(job_db.status),
                    created_at=job_db.created_at,
                    started_at=job_db.started_at,
                    completed_at=job_db.completed_at,
                    output=job_db.output,
                    error=job_db.error,
                    cpu_cores_allocated=job_db.cpu_cores_allocated,
                    memory_allocated=job_db.memory_allocated,
                    max_runtime=job_db.max_runtime
                ))
            return jobs

    async def get_user_running_jobs(self, username: str) -> List[Job]:
        """Get running jobs for a user."""
        async with self.async_session() as session:
            result = await session.execute(
                select(JobDB).where(
                    JobDB.user == username,
                    JobDB.status == JobStatus.RUNNING.value
                )
            )
            jobs = []
            for job_db in result.scalars():
                jobs.append(Job(
                    job_id=job_db.job_id,
                    cpp_file=job_db.cpp_file,
                    user=job_db.user,
                    priority=job_db.priority,
                    status=JobStatus(job_db.status),
                    created_at=job_db.created_at,
                    started_at=job_db.started_at,
                    completed_at=job_db.completed_at,
                    output=job_db.output,
                    error=job_db.error,
                    cpu_cores_allocated=job_db.cpu_cores_allocated,
                    memory_allocated=job_db.memory_allocated,
                    max_runtime=job_db.max_runtime
                ))
            return jobs