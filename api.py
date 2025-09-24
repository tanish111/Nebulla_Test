"""
FastAPI REST API for the CPU orchestrator daemon.
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional
import asyncio
import logging
from datetime import datetime

from models import Job, JobRequest, User, GPUMetrics, JobStatus
from database import DatabaseManager
from scheduler import JobScheduler
from metrics_generator import MockGPUMetricsGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="CPU Orchestrator Daemon",
    description="A CPU-only prototype of a GPU orchestration daemon inspired by Run:AI",
    version="1.0.0"
)

# Global instances
db_manager = DatabaseManager()
metrics_generator = MockGPUMetricsGenerator()
scheduler = JobScheduler(db_manager, max_concurrent_jobs=2)

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    logger.info("Starting CPU Orchestrator Daemon...")
    
    # Initialize database
    await db_manager.init_db()
    logger.info("Database initialized")
    
    # Start metrics generation
    asyncio.create_task(
        metrics_generator.start_generation(
            interval=3.0, 
            store_callback=db_manager.store_metrics
        )
    )
    logger.info("Metrics generation started")
    
    # Start job scheduler
    asyncio.create_task(scheduler.start_scheduler())
    logger.info("Job scheduler started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown."""
    logger.info("Shutting down CPU Orchestrator Daemon...")
    metrics_generator.stop_generation()
    scheduler.stop_scheduler()

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "CPU Orchestrator Daemon",
        "version": "1.0.0",
        "description": "A CPU-only prototype of a GPU orchestration daemon",
        "endpoints": {
            "metrics": "/metrics",
            "jobs": "/jobs",
            "users": "/users",
            "schedule": "/schedule",
            "status": "/status"
        }
    }

@app.get("/metrics", response_model=GPUMetrics)
async def get_metrics():
    """Get current mock GPU metrics."""
    try:
        metrics = metrics_generator.get_current_metrics()
        return metrics
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")

@app.get("/jobs", response_model=List[Job])
async def get_jobs(status: Optional[str] = None):
    """Get all jobs or jobs filtered by status."""
    try:
        if status:
            try:
                job_status = JobStatus(status.lower())
                jobs = await db_manager.get_jobs_by_status(job_status)
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid status. Valid options: {[s.value for s in JobStatus]}"
                )
        else:
            jobs = await db_manager.get_all_jobs()
        
        return jobs
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to get jobs")

@app.post("/jobs", response_model=Dict)
async def submit_job(job_request: JobRequest):
    """Submit a new job."""
    try:
        # Check if job ID already exists
        existing_job = await db_manager.get_job(job_request.job_id)
        if existing_job:
            raise HTTPException(status_code=400, detail=f"Job ID {job_request.job_id} already exists")
        
        # Check if user exists
        user = await db_manager.get_user(job_request.user)
        if not user:
            raise HTTPException(status_code=400, detail=f"User {job_request.user} not found")
        
        # Create job
        job = Job(
            job_id=job_request.job_id,
            cpp_file=job_request.cpp_file,
            user=job_request.user,
            priority=job_request.priority,
            max_runtime=job_request.max_runtime or 300
        )
        
        # Add job to scheduler
        success = await scheduler.add_job(job)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to add job to queue")
        
        # Simulate job load in metrics
        queue_status = await scheduler.get_queue_status()
        metrics_generator.simulate_job_load(queue_status["running_jobs"])
        
        logger.info(f"Job {job.job_id} submitted by user {job.user}")
        
        return {
            "message": f"Job {job.job_id} submitted successfully",
            "job_id": job.job_id,
            "status": job.status.value,
            "queue_position": len(await db_manager.get_jobs_by_status(JobStatus.QUEUED))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit job")

@app.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str):
    """Get a specific job by ID."""
    try:
        job = await db_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        return job
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get job")

@app.post("/jobs/{job_id}/cancel", response_model=Dict)
async def cancel_job(job_id: str):
    """Cancel a specific job."""
    try:
        # Check if job exists
        job = await db_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Check if job can be cancelled
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot cancel job in {job.status.value} status"
            )
        
        # Cancel the job
        success = await scheduler.cancel_job(job_id)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel job")
        
        logger.info(f"Job {job_id} cancelled")
        
        return {
            "message": f"Job {job_id} cancelled successfully",
            "job_id": job_id,
            "cancelled_at": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel job")

@app.post("/schedule", response_model=Dict)
async def trigger_manual_schedule():
    """Trigger manual scheduling (for testing/debugging)."""
    try:
        queue_status = await scheduler.get_queue_status()
        
        # Simulate high priority job load spike in metrics
        if queue_status["running_jobs"] > 0:
            metrics_generator.simulate_high_priority_job()
        
        return {
            "message": "Manual scheduling triggered",
            "timestamp": datetime.now().isoformat(),
            "queue_status": queue_status
        }
    except Exception as e:
        logger.error(f"Error triggering manual schedule: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger scheduling")

@app.get("/users", response_model=List[User])
async def get_users():
    """Get all users."""
    try:
        users = await db_manager.get_all_users()
        return users
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        raise HTTPException(status_code=500, detail="Failed to get users")

@app.get("/users/{username}", response_model=User)
async def get_user(username: str):
    """Get a specific user."""
    try:
        user = await db_manager.get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail=f"User {username} not found")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user {username}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user")

@app.post("/users", response_model=Dict)
async def create_user(user: User):
    """Create a new user."""
    try:
        # Check if user already exists
        existing_user = await db_manager.get_user(user.username)
        if existing_user:
            raise HTTPException(status_code=400, detail=f"User {user.username} already exists")
        
        # Create user
        success = await db_manager.create_user(user)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to create user")
        
        logger.info(f"User {user.username} created")
        
        return {
            "message": f"User {user.username} created successfully",
            "username": user.username,
            "created_at": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")

@app.get("/users/{username}/jobs", response_model=List[Job])
async def get_user_jobs(username: str):
    """Get all jobs for a specific user."""
    try:
        # Check if user exists
        user = await db_manager.get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail=f"User {username} not found")
        
        # Get all jobs for the user
        all_jobs = await db_manager.get_all_jobs()
        user_jobs = [job for job in all_jobs if job.user == username]
        
        return user_jobs
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting jobs for user {username}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user jobs")

@app.get("/status", response_model=Dict)
async def get_system_status():
    """Get overall system status."""
    try:
        queue_status = await scheduler.get_queue_status()
        metrics = metrics_generator.get_current_metrics()
        
        # Get job statistics
        all_jobs = await db_manager.get_all_jobs()
        job_stats = {
            "total": len(all_jobs),
            "queued": len([j for j in all_jobs if j.status == JobStatus.QUEUED]),
            "running": len([j for j in all_jobs if j.status == JobStatus.RUNNING]),
            "completed": len([j for j in all_jobs if j.status == JobStatus.COMPLETED]),
            "failed": len([j for j in all_jobs if j.status == JobStatus.FAILED]),
            "cancelled": len([j for j in all_jobs if j.status == JobStatus.CANCELLED])
        }
        
        return {
            "timestamp": datetime.now().isoformat(),
            "scheduler": queue_status,
            "metrics": metrics.dict(),
            "job_statistics": job_stats,
            "system_info": {
                "max_concurrent_jobs": scheduler.max_concurrent_jobs,
                "metrics_interval": "3.0 seconds",
                "database": "SQLite"
            }
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get system status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)