#!/usr/bin/env python3
"""
GPU Orchestration Daemon - Simple version using only standard library
A comprehensive job scheduling and resource management daemon with mock GPU metrics.
"""

import asyncio
import json
import logging
import os
import random
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import socketserver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daemon.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================== Data Models ==================

class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    PREEMPTED = "preempted"
    CANCELLED = "cancelled"
    FAILED = "failed"

class GPUMetrics:
    """Mock GPU metrics data structure"""
    def __init__(self, gpu_utilization=0.0, memory_used=0.0, memory_total=16.0, temperature=35.0):
        self.gpu_utilization = gpu_utilization
        self.memory_used = memory_used
        self.memory_total = memory_total
        self.temperature = temperature
        self.timestamp = datetime.now()
    
    def to_dict(self):
        return {
            "gpu_utilization": self.gpu_utilization,
            "memory_used": self.memory_used,
            "memory_total": self.memory_total,
            "memory_utilization": (self.memory_used / self.memory_total) * 100,
            "temperature": self.temperature,
            "timestamp": self.timestamp.isoformat()
        }

class Job:
    """Job data structure"""
    def __init__(self, job_id, status, priority, user, cpp_file=None, command=None, 
                 cpu_cores=1, gpu_memory=1.0):
        self.job_id = job_id
        self.status = status
        self.priority = priority
        self.user = user
        self.cpp_file = cpp_file
        self.command = command
        self.cpu_cores = cpu_cores
        self.gpu_memory = gpu_memory
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.logs = ""
        self.exit_code = None
    
    def to_dict(self):
        return {
            "job_id": self.job_id,
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "priority": self.priority,
            "user": self.user,
            "cpp_file": self.cpp_file,
            "command": self.command,
            "cpu_cores": self.cpu_cores,
            "gpu_memory": self.gpu_memory,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "logs": self.logs,
            "exit_code": self.exit_code
        }

class User:
    """User data structure with quotas"""
    def __init__(self, username, max_concurrent_jobs=5, max_cpu_cores=8, max_gpu_memory=8.0):
        self.username = username
        self.max_concurrent_jobs = max_concurrent_jobs
        self.max_cpu_cores = max_cpu_cores
        self.max_gpu_memory = max_gpu_memory
        self.current_jobs = 0
    
    def can_submit_job(self, cpu_cores, gpu_memory):
        return (self.current_jobs < self.max_concurrent_jobs and 
                cpu_cores <= self.max_cpu_cores and 
                gpu_memory <= self.max_gpu_memory)
    
    def to_dict(self):
        return {
            "username": self.username,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "max_cpu_cores": self.max_cpu_cores,
            "max_gpu_memory": self.max_gpu_memory,
            "current_jobs": self.current_jobs
        }

# ================== Database Layer ==================

class DatabaseManager:
    """SQLite database manager for job persistence"""
    
    def __init__(self, db_path="orchestrator.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    user TEXT NOT NULL,
                    cpp_file TEXT,
                    command TEXT,
                    cpu_cores INTEGER NOT NULL,
                    gpu_memory REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    logs TEXT,
                    exit_code INTEGER
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    max_concurrent_jobs INTEGER NOT NULL,
                    max_cpu_cores INTEGER NOT NULL,
                    max_gpu_memory REAL NOT NULL,
                    current_jobs INTEGER DEFAULT 0
                )
            ''')
            
            conn.commit()
        finally:
            conn.close()
    
    def save_job(self, job):
        """Save job to database"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR REPLACE INTO jobs 
                (job_id, status, priority, user, cpp_file, command, cpu_cores, gpu_memory,
                 created_at, started_at, completed_at, logs, exit_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.job_id, 
                job.status.value if isinstance(job.status, JobStatus) else job.status,
                job.priority, job.user, job.cpp_file, job.command, job.cpu_cores, job.gpu_memory,
                job.created_at.isoformat() if job.created_at else None,
                job.started_at.isoformat() if job.started_at else None,
                job.completed_at.isoformat() if job.completed_at else None,
                job.logs, job.exit_code
            ))
            conn.commit()
        finally:
            conn.close()
    
    def save_user(self, user):
        """Save user to database"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR REPLACE INTO users 
                (username, max_concurrent_jobs, max_cpu_cores, max_gpu_memory, current_jobs)
                VALUES (?, ?, ?, ?, ?)
            ''', (user.username, user.max_concurrent_jobs, user.max_cpu_cores, 
                  user.max_gpu_memory, user.current_jobs))
            conn.commit()
        finally:
            conn.close()
    
    def load_jobs(self):
        """Load all jobs from database"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute('SELECT * FROM jobs')
            jobs = []
            for row in cursor.fetchall():
                job = Job(
                    job_id=row[0],
                    status=JobStatus(row[1]),
                    priority=row[2],
                    user=row[3],
                    cpp_file=row[4],
                    command=row[5],
                    cpu_cores=row[6],
                    gpu_memory=row[7]
                )
                job.created_at = datetime.fromisoformat(row[8]) if row[8] else None
                job.started_at = datetime.fromisoformat(row[9]) if row[9] else None
                job.completed_at = datetime.fromisoformat(row[10]) if row[10] else None
                job.logs = row[11] or ""
                job.exit_code = row[12]
                jobs.append(job)
            return jobs
        finally:
            conn.close()
    
    def load_users(self):
        """Load all users from database"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute('SELECT * FROM users')
            users = []
            for row in cursor.fetchall():
                user = User(
                    username=row[0],
                    max_concurrent_jobs=row[1],
                    max_cpu_cores=row[2],
                    max_gpu_memory=row[3]
                )
                user.current_jobs = row[4]
                users.append(user)
            return users
        finally:
            conn.close()

# ================== GPU Metrics Generator ==================

class GPUMetricsGenerator:
    """Mock GPU metrics generator"""
    
    def __init__(self):
        self.current_metrics = GPUMetrics()
        self.running = False
        self.thread = None
    
    def start(self):
        """Start metrics generation"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._generate_metrics, daemon=True)
            self.thread.start()
            logger.info("GPU metrics generator started")
    
    def stop(self):
        """Stop metrics generation"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("GPU metrics generator stopped")
    
    def _generate_metrics(self):
        """Generate mock GPU metrics periodically"""
        while self.running:
            # Generate realistic-looking metrics with some correlation
            base_util = random.uniform(0, 100)
            
            # Memory usage somewhat correlates with utilization
            memory_factor = (base_util / 100) * 0.7 + random.uniform(0, 0.3)
            memory_used = min(16.0, memory_factor * 16.0)
            
            # Temperature correlates with utilization
            temp_base = 30 + (base_util / 100) * 40  # 30-70°C based on util
            temperature = temp_base + random.uniform(-5, 15)  # Add some noise
            temperature = max(30, min(90, temperature))  # Clamp to realistic range
            
            self.current_metrics = GPUMetrics(
                gpu_utilization=base_util,
                memory_used=memory_used,
                memory_total=16.0,
                temperature=temperature
            )
            
            # Random interval between 2-5 seconds
            time.sleep(random.uniform(2, 5))
    
    def get_current_metrics(self):
        """Get current metrics"""
        return self.current_metrics

# ================== Job Scheduler ==================

class JobScheduler:
    """FIFO + Priority-based job scheduler with preemption"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.jobs = {}
        self.users = {}
        self.running_jobs = set()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.lock = threading.Lock()
        
        # Load existing data
        self._load_from_database()
        
        # Create default admin user
        if "admin" not in self.users:
            self.add_user(User("admin", max_concurrent_jobs=10, max_cpu_cores=16, max_gpu_memory=16.0))
    
    def _load_from_database(self):
        """Load jobs and users from database"""
        jobs = self.db_manager.load_jobs()
        for job in jobs:
            self.jobs[job.job_id] = job
            if job.status == JobStatus.RUNNING:
                # Reset running jobs on restart
                job.status = JobStatus.QUEUED
                self.db_manager.save_job(job)
        
        users = self.db_manager.load_users()
        for user in users:
            # Recalculate current jobs
            user.current_jobs = sum(1 for job in self.jobs.values() 
                                  if job.user == user.username and 
                                  job.status in [JobStatus.QUEUED, JobStatus.RUNNING])
            self.users[user.username] = user
            self.db_manager.save_user(user)
    
    def add_user(self, user):
        """Add or update user"""
        with self.lock:
            self.users[user.username] = user
            self.db_manager.save_user(user)
            logger.info(f"Added/updated user: {user.username}")
            return True
    
    def get_user(self, username):
        """Get user by username"""
        return self.users.get(username)
    
    def get_all_users(self):
        """Get all users"""
        return list(self.users.values())
    
    def submit_job(self, job_data):
        """Submit a new job"""
        with self.lock:
            # Check if user exists
            user = self.users.get(job_data.get('user'))
            if not user:
                raise ValueError(f"User {job_data.get('user')} not found")
            
            cpu_cores = job_data.get('cpu_cores', 1)
            gpu_memory = job_data.get('gpu_memory', 1.0)
            
            # Check user quotas
            if not user.can_submit_job(cpu_cores, gpu_memory):
                raise ValueError(f"User {job_data.get('user')} quota exceeded")
            
            # Generate job ID if not provided
            job_id = job_data.get('job_id') or f"job_{uuid.uuid4().hex[:8]}"
            
            # Check for duplicate job ID
            if job_id in self.jobs:
                raise ValueError(f"Job ID {job_id} already exists")
            
            # Create job
            job = Job(
                job_id=job_id,
                status=JobStatus.QUEUED,
                priority=job_data.get('priority', 1),
                user=job_data.get('user'),
                cpp_file=job_data.get('cpp_file'),
                command=job_data.get('command'),
                cpu_cores=cpu_cores,
                gpu_memory=gpu_memory
            )
            
            self.jobs[job_id] = job
            user.current_jobs += 1
            
            # Save to database
            self.db_manager.save_job(job)
            self.db_manager.save_user(user)
            
            logger.info(f"Job {job_id} submitted by user {job_data.get('user')}")
            
            # Trigger scheduling
            self._schedule_jobs()
            
            return job_id
    
    def cancel_job(self, job_id):
        """Cancel a job"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return False
            
            if job.status == JobStatus.RUNNING:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now()
                self.running_jobs.discard(job_id)
                logger.info(f"Cancelled running job {job_id}")
            elif job.status == JobStatus.QUEUED:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now()
                logger.info(f"Cancelled queued job {job_id}")
            else:
                return False  # Job already completed/cancelled
            
            # Update user current jobs count
            user = self.users.get(job.user)
            if user:
                user.current_jobs = max(0, user.current_jobs - 1)
                self.db_manager.save_user(user)
            
            self.db_manager.save_job(job)
            self._schedule_jobs()  # Try to schedule next jobs
            
            return True
    
    def _schedule_jobs(self):
        """Schedule jobs based on priority and FIFO"""
        # Get queued jobs sorted by priority (desc) then creation time (asc)
        queued_jobs = [job for job in self.jobs.values() if job.status == JobStatus.QUEUED]
        queued_jobs.sort(key=lambda j: (-j.priority, j.created_at))
        
        # Check for preemption opportunities
        self._check_preemption(queued_jobs)
        
        # Schedule new jobs
        for job in queued_jobs:
            if len(self.running_jobs) >= 4:  # Max concurrent jobs
                break
                
            if self._can_run_job(job):
                self._start_job(job)
    
    def _check_preemption(self, queued_jobs):
        """Check if higher priority jobs should preempt running jobs"""
        if not queued_jobs:
            return
        
        running_jobs = [job for job in self.jobs.values() if job.status == JobStatus.RUNNING]
        if not running_jobs:
            return
        
        # Find lowest priority running job
        lowest_priority_job = min(running_jobs, key=lambda j: j.priority)
        highest_priority_queued = queued_jobs[0]
        
        # Preempt if queued job has higher priority
        if highest_priority_queued.priority > lowest_priority_job.priority:
            self._preempt_job(lowest_priority_job)
            logger.info(f"Preempted job {lowest_priority_job.job_id} for higher priority job {highest_priority_queued.job_id}")
    
    def _can_run_job(self, job):
        """Check if job can be run based on resource availability"""
        return len(self.running_jobs) < 4
    
    def _start_job(self, job):
        """Start executing a job"""
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now()
        self.running_jobs.add(job.job_id)
        
        self.db_manager.save_job(job)
        
        # Submit job for execution
        future = self.executor.submit(self._execute_job, job)
        future.add_done_callback(lambda f: self._job_completed(job.job_id, f))
        
        logger.info(f"Started job {job.job_id}")
    
    def _preempt_job(self, job):
        """Preempt a running job"""
        job.status = JobStatus.PREEMPTED
        job.completed_at = datetime.now()
        self.running_jobs.discard(job.job_id)
        
        # Update user current jobs count
        user = self.users.get(job.user)
        if user:
            user.current_jobs = max(0, user.current_jobs - 1)
            self.db_manager.save_user(user)
        
        self.db_manager.save_job(job)
    
    def _execute_job(self, job):
        """Execute a job (CPU task)"""
        try:
            start_time = time.time()
            
            if job.cpp_file:
                # Compile and run C++ file
                result = self._execute_cpp_job(job)
            elif job.command:
                # Execute custom command
                result = self._execute_command_job(job)
            else:
                raise ValueError("No cpp_file or command specified")
            
            execution_time = time.time() - start_time
            
            return result, execution_time
            
        except Exception as e:
            logger.error(f"Job {job.job_id} execution failed: {str(e)}")
            return (1, str(e)), 0
    
    def _execute_cpp_job(self, job):
        """Execute C++ compilation and execution"""
        cpp_path = Path(job.cpp_file)
        if not cpp_path.exists():
            raise FileNotFoundError(f"C++ file {job.cpp_file} not found")
        
        executable_path = cpp_path.with_suffix('')
        
        # Compile
        compile_cmd = ['g++', '-o', str(executable_path), str(cpp_path)]
        compile_result = subprocess.run(compile_cmd, capture_output=True, text=True)
        
        if compile_result.returncode != 0:
            return compile_result.returncode, f"Compilation failed:\n{compile_result.stderr}"
        
        # Execute
        exec_result = subprocess.run([str(executable_path)], capture_output=True, text=True)
        
        # Clean up executable
        if executable_path.exists():
            executable_path.unlink()
        
        output = f"Compilation successful\n--- Execution Output ---\n{exec_result.stdout}"
        if exec_result.stderr:
            output += f"\n--- Stderr ---\n{exec_result.stderr}"
        
        return exec_result.returncode, output
    
    def _execute_command_job(self, job):
        """Execute custom command"""
        result = subprocess.run(job.command, shell=True, capture_output=True, text=True)
        
        output = f"--- Command Output ---\n{result.stdout}"
        if result.stderr:
            output += f"\n--- Stderr ---\n{result.stderr}"
        
        return result.returncode, output
    
    def _job_completed(self, job_id, future):
        """Handle job completion"""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            
            try:
                (exit_code, logs), execution_time = future.result()
                
                job.status = JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
                job.exit_code = exit_code
                job.logs = logs
                job.completed_at = datetime.now()
                
                logger.info(f"Job {job_id} completed with exit code {exit_code} in {execution_time:.2f}s")
                
            except Exception as e:
                job.status = JobStatus.FAILED
                job.exit_code = 1
                job.logs = f"Execution error: {str(e)}"
                job.completed_at = datetime.now()
                
                logger.error(f"Job {job_id} failed with error: {str(e)}")
            
            finally:
                self.running_jobs.discard(job_id)
                
                # Update user current jobs count
                user = self.users.get(job.user)
                if user:
                    user.current_jobs = max(0, user.current_jobs - 1)
                    self.db_manager.save_user(user)
                
                self.db_manager.save_job(job)
                
                # Try to schedule next jobs
                self._schedule_jobs()
    
    def get_job(self, job_id):
        """Get job by ID"""
        return self.jobs.get(job_id)
    
    def get_all_jobs(self):
        """Get all jobs"""
        return list(self.jobs.values())
    
    def manual_schedule(self):
        """Manually trigger scheduling"""
        with self.lock:
            self._schedule_jobs()
            logger.info("Manual scheduling triggered")

# ================== HTTP API Server ==================

class APIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the API"""
    
    def __init__(self, *args, orchestrator=None, **kwargs):
        self.orchestrator = orchestrator
        super().__init__(*args, **kwargs)
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        """Set response headers"""
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response"""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data).encode())
    
    def _send_error_response(self, message, status_code=400):
        """Send error response"""
        self._send_json_response({"error": message}, status_code)
    
    def _get_post_data(self):
        """Get POST data as dict"""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            post_data = self.rfile.read(content_length)
            return json.loads(post_data.decode())
        return {}
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests"""
        self._set_headers()
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        try:
            if path == '/':
                self._send_json_response({
                    "name": "GPU Orchestration Daemon",
                    "version": "1.0.0",
                    "status": "running",
                    "endpoints": {
                        "metrics": "/metrics",
                        "jobs": "/jobs",
                        "users": "/users",
                        "schedule": "/schedule"
                    }
                })
            elif path == '/metrics':
                metrics = self.orchestrator.gpu_metrics.get_current_metrics()
                self._send_json_response(metrics.to_dict())
            elif path == '/jobs':
                jobs = self.orchestrator.scheduler.get_all_jobs()
                self._send_json_response([job.to_dict() for job in jobs])
            elif path == '/users':
                users = self.orchestrator.scheduler.get_all_users()
                self._send_json_response([user.to_dict() for user in users])
            else:
                self._send_error_response("Not found", 404)
        except Exception as e:
            logger.error(f"GET error: {str(e)}")
            self._send_error_response(f"Internal server error: {str(e)}", 500)
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        try:
            if path == '/jobs':
                # Submit job
                data = self._get_post_data()
                job_id = self.orchestrator.scheduler.submit_job(data)
                self._send_json_response({"job_id": job_id, "status": "submitted"})
            elif path.startswith('/jobs/') and path.endswith('/cancel'):
                # Cancel job
                job_id = path.split('/')[-2]
                success = self.orchestrator.scheduler.cancel_job(job_id)
                if success:
                    self._send_json_response({"job_id": job_id, "status": "cancelled"})
                else:
                    self._send_error_response("Job not found or cannot be cancelled", 404)
            elif path == '/schedule':
                # Manual scheduling
                self.orchestrator.scheduler.manual_schedule()
                self._send_json_response({"status": "scheduling triggered"})
            elif path == '/users':
                # Create user
                data = self._get_post_data()
                user = User(
                    username=data.get('username'),
                    max_concurrent_jobs=data.get('max_concurrent_jobs', 5),
                    max_cpu_cores=data.get('max_cpu_cores', 8),
                    max_gpu_memory=data.get('max_gpu_memory', 8.0)
                )
                self.orchestrator.scheduler.add_user(user)
                self._send_json_response({"username": user.username, "status": "created/updated"})
            else:
                self._send_error_response("Not found", 404)
        except ValueError as e:
            self._send_error_response(str(e), 400)
        except Exception as e:
            logger.error(f"POST error: {str(e)}")
            self._send_error_response(f"Internal server error: {str(e)}", 500)
    
    def log_message(self, format, *args):
        """Override to reduce log noise"""
        pass

# ================== Main Daemon Class ==================

class OrchestrationDaemon:
    """Main orchestration daemon"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.gpu_metrics = GPUMetricsGenerator()
        self.scheduler = JobScheduler(self.db_manager)
        self.server = None
        logger.info("Orchestration daemon initialized")
    
    def start(self, host='localhost', port=8000):
        """Start the daemon"""
        self.gpu_metrics.start()
        
        # Create HTTP server with orchestrator reference
        handler = lambda *args, **kwargs: APIHandler(*args, orchestrator=self, **kwargs)
        self.server = HTTPServer((host, port), handler)
        
        logger.info(f"Starting HTTP server on {host}:{port}")
        logger.info("Orchestration daemon started")
        
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop the daemon"""
        if self.server:
            self.server.shutdown()
        self.gpu_metrics.stop()
        logger.info("Orchestration daemon stopped")

# ================== Main Entry Point ==================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="GPU Orchestration Daemon")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    
    args = parser.parse_args()
    
    logger.info(f"Starting GPU Orchestration Daemon on {args.host}:{args.port}")
    
    daemon = OrchestrationDaemon()
    daemon.start(args.host, args.port)