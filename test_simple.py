#!/usr/bin/env python3
"""
Simple test script for the GPU Orchestration Daemon using only standard library
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error

BASE_URL = "http://localhost:8000"

def make_request(url, method='GET', data=None):
    """Make HTTP request using urllib"""
    if data:
        data = json.dumps(data).encode()
    
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Content-Type', 'application/json')
    
    try:
        with urllib.request.urlopen(req) as response:
            return response.getcode(), json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.reason}
    except Exception as e:
        return 500, {"error": str(e)}

def test_api():
    """Test the daemon API endpoints"""
    print("🚀 Testing GPU Orchestration Daemon API (Simple Version)")
    print("=" * 60)
    
    # Test root endpoint
    print("\n1. Testing root endpoint...")
    status, response = make_request(f"{BASE_URL}/")
    print(f"Status: {status}")
    print(f"Response: {json.dumps(response, indent=2)}")
    
    if status != 200:
        print("❌ Daemon not responding. Is it running?")
        return
    
    # Test metrics endpoint
    print("\n2. Testing metrics endpoint...")
    status, response = make_request(f"{BASE_URL}/metrics")
    print(f"Status: {status}")
    print(f"GPU Metrics: {json.dumps(response, indent=2)}")
    
    # Create test users
    print("\n3. Creating test users...")
    users = [
        {"username": "alice", "max_concurrent_jobs": 3, "max_cpu_cores": 4, "max_gpu_memory": 8.0},
        {"username": "bob", "max_concurrent_jobs": 2, "max_cpu_cores": 2, "max_gpu_memory": 4.0}
    ]
    
    for user in users:
        status, response = make_request(f"{BASE_URL}/users", method='POST', data=user)
        print(f"Created user {user['username']}: {status} - {response}")
    
    # List users
    print("\n4. Listing users...")
    status, response = make_request(f"{BASE_URL}/users")
    print(f"Users ({status}): {json.dumps(response, indent=2)}")
    
    # Submit test jobs
    print("\n5. Submitting test jobs...")
    jobs = [
        {
            "cpp_file": "examples/hello.cpp",
            "user": "alice",
            "priority": 2,
            "cpu_cores": 1,
            "gpu_memory": 1.0
        },
        {
            "cpp_file": "examples/compute.cpp",
            "user": "alice",
            "priority": 1,
            "cpu_cores": 2,
            "gpu_memory": 2.0
        },
        {
            "command": "echo 'Hello from command job!' && sleep 3 && echo 'Command job completed!'",
            "user": "bob",
            "priority": 1,
            "cpu_cores": 1,
            "gpu_memory": 0.5
        }
    ]
    
    job_ids = []
    for job in jobs:
        status, response = make_request(f"{BASE_URL}/jobs", method='POST', data=job)
        if status == 200:
            job_id = response["job_id"]
            job_ids.append(job_id)
            print(f"Submitted job {job_id}: {response}")
        else:
            print(f"Failed to submit job: {status} - {response}")
    
    # Monitor jobs
    print("\n6. Monitoring jobs...")
    for i in range(15):
        print(f"\n--- Check {i+1} ---")
        status, jobs_response = make_request(f"{BASE_URL}/jobs")
        
        if status == 200:
            jobs = jobs_response
            for job in jobs:
                job_status = job["status"]
                job_id = job["job_id"]
                user = job["user"]
                print(f"Job {job_id} ({user}): {job_status}")
                
                if job_status in ["completed", "failed"] and job.get("logs"):
                    logs = job["logs"]
                    print(f"  Logs: {logs[:200]}{'...' if len(logs) > 200 else ''}")
            
            # Check if all jobs are done
            statuses = [job["status"] for job in jobs]
            if all(status in ["completed", "failed", "cancelled"] for status in statuses):
                print("\nAll jobs completed!")
                break
        else:
            print(f"Error getting jobs: {status}")
        
        time.sleep(2)
    
    # Test job cancellation
    print(f"\n7. Testing job cancellation...")
    # Submit a long-running job
    long_job = {
        "command": "sleep 30 && echo 'Long job done'",
        "user": "alice",
        "priority": 1,
        "cpu_cores": 1,
        "gpu_memory": 1.0
    }
    status, response = make_request(f"{BASE_URL}/jobs", method='POST', data=long_job)
    if status == 200:
        cancel_job_id = response["job_id"]
        print(f"Submitted long job {cancel_job_id}")
        
        time.sleep(2)  # Let it start
        
        # Cancel the job
        status, response = make_request(f"{BASE_URL}/jobs/{cancel_job_id}/cancel", method='POST')
        print(f"Cancelled job {cancel_job_id}: {status} - {response}")
    
    # Test manual scheduling
    print("\n8. Testing manual scheduling...")  
    status, response = make_request(f"{BASE_URL}/schedule", method='POST')
    print(f"Manual schedule trigger: {status} - {response}")
    
    # Final job status
    print("\n9. Final job status...")
    status, jobs_response = make_request(f"{BASE_URL}/jobs")
    
    if status == 200:
        jobs = jobs_response
        print(f"Total jobs: {len(jobs)}")
        status_counts = {}
        for job in jobs:
            job_status = job["status"]
            status_counts[job_status] = status_counts.get(job_status, 0) + 1
        
        for status_name, count in status_counts.items():
            print(f"  {status_name}: {count}")
    
    # Test metrics again
    print("\n10. Final metrics check...")
    status, response = make_request(f"{BASE_URL}/metrics")
    if status == 200:
        print(f"Final GPU Metrics: {json.dumps(response, indent=2)}")
    
    print("\n✅ API testing completed successfully!")

if __name__ == "__main__":
    test_api()