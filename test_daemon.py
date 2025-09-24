#!/usr/bin/env python3
"""
Test script for the GPU Orchestration Daemon
Demonstrates API usage and functionality
"""

import requests
import json
import time
import sys

BASE_URL = "http://localhost:8000"

def test_api():
    """Test the daemon API endpoints"""
    print("🚀 Testing GPU Orchestration Daemon API")
    print("=" * 50)
    
    try:
        # Test root endpoint
        print("\n1. Testing root endpoint...")
        response = requests.get(f"{BASE_URL}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Test metrics endpoint
        print("\n2. Testing metrics endpoint...")
        response = requests.get(f"{BASE_URL}/metrics")
        print(f"Status: {response.status_code}")
        print(f"GPU Metrics: {json.dumps(response.json(), indent=2)}")
        
        # Create test users
        print("\n3. Creating test users...")
        users = [
            {"username": "alice", "max_concurrent_jobs": 3, "max_cpu_cores": 4, "max_gpu_memory": 8.0},
            {"username": "bob", "max_concurrent_jobs": 2, "max_cpu_cores": 2, "max_gpu_memory": 4.0}
        ]
        
        for user in users:
            response = requests.post(f"{BASE_URL}/users", json=user)
            print(f"Created user {user['username']}: {response.status_code}")
        
        # List users
        print("\n4. Listing users...")
        response = requests.get(f"{BASE_URL}/users")
        print(f"Users: {json.dumps(response.json(), indent=2)}")
        
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
                "cpp_file": "examples/error.cpp",
                "user": "bob",
                "priority": 3,
                "cpu_cores": 1,
                "gpu_memory": 1.0
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
            response = requests.post(f"{BASE_URL}/jobs", json=job)
            if response.status_code == 200:
                job_id = response.json()["job_id"]
                job_ids.append(job_id)
                print(f"Submitted job {job_id}: {response.json()}")
            else:
                print(f"Failed to submit job: {response.status_code} - {response.text}")
        
        # Monitor jobs
        print("\n6. Monitoring jobs...")
        for i in range(10):
            print(f"\n--- Check {i+1} ---")
            response = requests.get(f"{BASE_URL}/jobs")
            jobs = response.json()
            
            for job in jobs:
                status = job["status"]
                job_id = job["job_id"]
                user = job["user"]
                print(f"Job {job_id} ({user}): {status}")
                
                if status in ["completed", "failed"] and job.get("logs"):
                    print(f"  Logs: {job['logs'][:200]}{'...' if len(job['logs']) > 200 else ''}")
            
            # Check if all jobs are done
            statuses = [job["status"] for job in jobs]
            if all(status in ["completed", "failed", "cancelled"] for status in statuses):
                print("\nAll jobs completed!")
                break
            
            time.sleep(2)
        
        # Test job cancellation
        if job_ids:
            print(f"\n7. Testing job cancellation...")
            # Submit a long-running job
            long_job = {
                "command": "sleep 30 && echo 'Long job done'",
                "user": "alice",
                "priority": 1,
                "cpu_cores": 1,
                "gpu_memory": 1.0
            }
            response = requests.post(f"{BASE_URL}/jobs", json=long_job)
            if response.status_code == 200:
                cancel_job_id = response.json()["job_id"]
                print(f"Submitted long job {cancel_job_id}")
                
                time.sleep(2)  # Let it start
                
                # Cancel the job
                response = requests.post(f"{BASE_URL}/jobs/{cancel_job_id}/cancel")
                print(f"Cancelled job {cancel_job_id}: {response.status_code}")
        
        # Test manual scheduling
        print("\n8. Testing manual scheduling...")
        response = requests.post(f"{BASE_URL}/schedule")
        print(f"Manual schedule trigger: {response.status_code}")
        
        # Final job status
        print("\n9. Final job status...")
        response = requests.get(f"{BASE_URL}/jobs")
        jobs = response.json()
        
        print(f"Total jobs: {len(jobs)}")
        for status in ["queued", "running", "completed", "failed", "cancelled", "preempted"]:
            count = sum(1 for job in jobs if job["status"] == status)
            if count > 0:
                print(f"  {status}: {count}")
        
        # Test metrics again
        print("\n10. Final metrics check...")
        response = requests.get(f"{BASE_URL}/metrics")
        print(f"Final GPU Metrics: {json.dumps(response.json(), indent=2)}")
        
        print("\n✅ API testing completed successfully!")
        
    except requests.exceptions.ConnectionError:
        print("❌ Error: Could not connect to daemon. Is it running?")
        print("Start the daemon with: python daemon.py")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    test_api()