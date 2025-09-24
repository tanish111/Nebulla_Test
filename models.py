"""
Core data models for the CPU orchestrator daemon.
"""
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"

class GPUMetrics(BaseModel):
    """Mock GPU metrics data structure."""
    gpu_utilization: float = Field(ge=0, le=100, description="GPU utilization percentage")
    memory_used: float = Field(ge=0, description="GPU memory used in GB")
    memory_total: float = Field(ge=0, description="Total GPU memory in GB")
    temperature: float = Field(ge=0, le=100, description="GPU temperature in Celsius")
    timestamp: datetime = Field(default_factory=datetime.now)

class JobRequest(BaseModel):
    """Request model for job submission."""
    job_id: str = Field(description="Unique job identifier")
    cpp_file: str = Field(description="Path to C++ file to compile and run")
    user: str = Field(description="Username submitting the job")
    priority: int = Field(default=1, ge=1, le=10, description="Job priority (1=lowest, 10=highest)")
    max_runtime: Optional[int] = Field(default=300, description="Maximum runtime in seconds")

class Job(BaseModel):
    """Job data structure."""
    job_id: str
    cpp_file: str
    user: str
    priority: int = 1
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    output: Optional[str] = None
    error: Optional[str] = None
    cpu_cores_allocated: int = 1
    memory_allocated: float = 1.0  # GB
    max_runtime: int = 300

class User(BaseModel):
    """User data structure with quotas."""
    username: str
    max_concurrent_jobs: int = 3
    max_cpu_cores: int = 4
    max_memory_gb: float = 8.0
    created_at: datetime = Field(default_factory=datetime.now)

# SQLAlchemy models for persistence
class JobDB(Base):
    __tablename__ = "jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, unique=True, nullable=False)
    cpp_file = Column(String, nullable=False)
    user = Column(String, nullable=False)
    priority = Column(Integer, default=1)
    status = Column(String, default=JobStatus.QUEUED.value)
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    output = Column(String, nullable=True)
    error = Column(String, nullable=True)
    cpu_cores_allocated = Column(Integer, default=1)
    memory_allocated = Column(Float, default=1.0)
    max_runtime = Column(Integer, default=300)

class UserDB(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, nullable=False)
    max_concurrent_jobs = Column(Integer, default=3)
    max_cpu_cores = Column(Integer, default=4)
    max_memory_gb = Column(Float, default=8.0)
    created_at = Column(DateTime, default=datetime.now)

class MetricsDB(Base):
    __tablename__ = "metrics"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    gpu_utilization = Column(Float, nullable=False)
    memory_used = Column(Float, nullable=False)
    memory_total = Column(Float, nullable=False)
    temperature = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.now)