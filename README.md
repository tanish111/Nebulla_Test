# CPU-Orchestrator Daemon (Run:AI Prototype)

A CPU-only prototype of a GPU orchestration daemon inspired by Run:AI, designed for job scheduling, resource tracking, and monitoring. It runs CPU tasks (e.g., compiling/running .cpp files) and mocks GPU metrics for testing, while providing an open API for frontend integration.

## Features

- **Mock GPU Metrics**: Periodically generates fake GPU utilization, memory usage, and temperature.
- **Job Scheduling**:
  - Submit CPU tasks as jobs.
  - FIFO and priority-based scheduling.
  - Job preemption for higher-priority tasks.
- **Resource Management**:
  - Mock GPU memory and CPU core allocation.
  - User-specific quotas and limits.
- **Job Tracking & Logging**:
  - Tracks job status: queued, running, completed, preempted.
  - Logs job execution times and mock resource usage.
- **REST API Endpoints**:
  - `/metrics` → Get current mock GPU stats.
  - `/jobs` → List all jobs and statuses.
  - `/jobs` (POST) → Submit a new job.
  - `/jobs/{id}/cancel` → Cancel a running job.
  - `/schedule` → Trigger scheduling manually.
  - `/users` → Manage user quotas and job limits.
- **Modular Design**: Easy to replace mock GPU metrics with real NVIDIA integration (nvidia-smi or NVML).

## Getting Started

### Prerequisites
- Python 3.9+
- GCC compiler (g++) for compiling .cpp jobs
- SQLite (built into Python, for persistent job history)

### Installation

```bash
git clone <repo_url>
cd Nebulla_Test
pip install -r requirements.txt
```

### Running the Daemon

**Option 1: Full-featured daemon (requires FastAPI)**
```bash
python daemon.py
```

**Option 2: Simple daemon (uses only standard library)**
```bash
python daemon_simple.py
```

The daemon will start generating mock GPU metrics and serve REST endpoints on http://localhost:8000.
## API Usage Examples

### Get current metrics
```bash
curl http://localhost:8000/metrics
```

### Submit a job
```bash
curl -X POST http://localhost:8000/jobs \
-H "Content-Type: application/json" \
-d '{"job_id": "job1", "cpp_file": "examples/hello.cpp", "user": "alice", "priority": 1}'
```

### List jobs
```bash
curl http://localhost:8000/jobs
```

### Cancel a job
```bash
curl -X POST http://localhost:8000/jobs/job1/cancel
```

### Create a user
```bash
curl -X POST http://localhost:8000/users \
-H "Content-Type: application/json" \
-d '{"username": "alice", "max_concurrent_jobs": 3, "max_cpu_cores": 4, "max_gpu_memory": 8.0}'
```

## Testing the Daemon

Run the included test script:
```bash
python test_simple.py
```

This will:
1. Test all API endpoints
2. Create test users
3. Submit various types of jobs (C++ compilation, shell commands)
4. Monitor job execution
5. Test job cancellation
6. Verify scheduler functionality

## Architecture Overview

```
Client / UI  <--->  REST API  <--->  Scheduler & Job Queue  <--->  Daemon Core
                                            |
                                            v
                                    Job Execution (CPU)
                                            |
                                            v
                                    SQLite Database
```

## Core Components

### 1. Mock GPU Metrics Generator
- Generates realistic GPU utilization (0-100%)
- Simulates memory usage (0-16GB)
- Mock temperature readings (30-90°C)
- Updates every 2-5 seconds with correlated values

### 2. Job Scheduler
- **FIFO + Priority**: Higher priority jobs scheduled first, then by creation time
- **Preemption**: Running jobs can be preempted by higher priority jobs
- **Resource Awareness**: Considers CPU cores and mock GPU memory requirements
- **User Quotas**: Enforces per-user limits on concurrent jobs and resources

### 3. Job Execution Engine
- **C++ Compilation**: Compiles and runs .cpp files using g++
- **Shell Commands**: Executes arbitrary shell commands
- **Asynchronous Execution**: Jobs run in background without blocking the API
- **Resource Tracking**: Monitors execution time and resource usage

### 4. Database Layer
- **SQLite**: Persistent storage for jobs, users, and metrics history
- **Job History**: Complete audit trail of all job executions
- **User Management**: Stores user quotas and current resource usage

### 5. REST API Server
- **Standard Library Version**: Uses Python's built-in http.server
- **FastAPI Version**: Full-featured API with automatic documentation
- **CORS Enabled**: Cross-origin requests supported for web frontends

## Job States

- **QUEUED**: Job submitted and waiting for resources
- **RUNNING**: Job currently executing
- **COMPLETED**: Job finished successfully (exit code 0)
- **FAILED**: Job finished with error (non-zero exit code)
- **CANCELLED**: Job manually cancelled by user
- **PREEMPTED**: Job stopped to make room for higher priority job

## User Quota System

Each user has configurable limits:
- **max_concurrent_jobs**: Maximum number of jobs running simultaneously
- **max_cpu_cores**: Maximum CPU cores that can be requested per job
- **max_gpu_memory**: Maximum mock GPU memory that can be requested per job

## Example Jobs

The repository includes example C++ files in the `examples/` directory:
- `hello.cpp`: Simple hello world with sleep simulation
- `compute.cpp`: CPU-intensive mathematical computation
- `error.cpp`: Demonstrates error handling and non-zero exit codes

## File Structure

```
.
├── daemon.py              # Full-featured daemon with FastAPI
├── daemon_simple.py       # Simple daemon using standard library only
├── test_simple.py         # Test script for API validation
├── requirements.txt       # Python dependencies
├── examples/              # Example C++ files for testing
│   ├── hello.cpp
│   ├── compute.cpp
│   └── error.cpp
├── orchestrator.db        # SQLite database (created on first run)
├── daemon.log            # Application logs
└── README.md             # This file
```
## Future Enhancements
- Replace mock GPU stats with real nvidia-smi or NVML integration.
- Multi-node orchestration and metrics aggregation.
- Prometheus-compatible endpoints and dashboards.
- Web-based dashboard for job monitoring and management.
- Container support for job isolation.
- Advanced scheduling algorithms (fair-share, backfill, etc.).

## License

This project is a prototype implementation for educational and testing purposes.
