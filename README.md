<p>CPU-Orchestrator Daemon (Run:AI Prototype)</p>
<p>A CPU-only prototype of a GPU orchestration daemon inspired by Run:AI, designed for job scheduling, resource tracking, and monitoring. It runs CPU tasks (e.g., compiling/running .cpp files) and mocks GPU metrics for testing, while providing an open API for frontend integration.</p>
<p>⸻</p>
<p>Features
•	Mock GPU Metrics: Periodically generates fake GPU utilization, memory usage, and temperature.
•	Job Scheduling:
•	Submit CPU tasks as jobs.
•	FIFO and priority-based scheduling.
•	Job preemption for higher-priority tasks.
•	Resource Management:
•	Mock GPU memory and CPU core allocation.
•	User-specific quotas and limits.
•	Job Tracking &amp; Logging:
•	Tracks job status: queued, running, completed, preempted.
•	Logs job execution times and mock resource usage.
•	REST API Endpoints:
•	/metrics → Get current mock GPU stats.
•	/jobs → List all jobs and statuses.
•	/jobs (POST) → Submit a new job.
•	/jobs/{id}/cancel → Cancel a running job.
•	/schedule → Trigger scheduling manually.
•	/users → Manage user quotas and job limits.
•	Modular Design: Easy to replace mock GPU metrics with real NVIDIA integration (nvidia-smi or NVML).</p>
<p>⸻</p>
<p>Getting Started</p>
<p>Prerequisites
•	Python 3.9+
•	FastAPI or Flask
•	SQLite (optional, for persistent job history)
•	GCC compiler (g++) for compiling .cpp jobs</p>
<p>Installation</p>
<p>git clone &lt;repo_url&gt;
cd cpu-orchestrator
pip install -r requirements.txt</p>
<p>Running the Daemon</p>
<p>python daemon.py</p>
<p>The daemon will start generating mock GPU metrics and serve REST endpoints on <a href="http://localhost:8000">http://localhost:8000</a>.</p>
<p>⸻</p>
<p>API Usage Examples
•	Get current metrics</p>
<p>curl <a href="http://localhost:8000/metrics">http://localhost:8000/metrics</a></p>
<pre><code>•	Submit a job
</code></pre>
<p>curl -X POST <a href="http://localhost:8000/jobs">http://localhost:8000/jobs</a> <br>
-H "Content-Type: application/json" <br>
-d '{"job_id": "job1", "cpp_file": "example.cpp", "user": "alice", "priority": 1}'</p>
<pre><code>•	List jobs
</code></pre>
<p>curl <a href="http://localhost:8000/jobs">http://localhost:8000/jobs</a></p>
<pre><code>•	Cancel a job
</code></pre>
<p>curl -X POST <a href="http://localhost:8000/jobs/job1/cancel">http://localhost:8000/jobs/job1/cancel</a></p>
<p>⸻</p>
<p>Architecture Overview</p>
<p>Client / UI  &lt;---&gt;  REST API  &lt;---&gt;  Scheduler &amp; Job Queue  &lt;---&gt;  Daemon Core
|
v
Job Execution (CPU)
|
v
In-memory / SQLite DB</p>
<p>⸻</p>
<p>Future Enhancements
•	Replace mock GPU stats with real nvidia-smi or NVML integration.
•	Multi-node orchestration and metrics aggregation.
•	Prometheus-compatible endpoints and dashboards.</p>
