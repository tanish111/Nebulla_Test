#!/usr/bin/env python3
"""
Test script for the CPU Orchestrator Daemon.
"""
import asyncio
import aiohttp
import json
import time

API_BASE = "http://localhost:8000"

async def test_api():
    """Test the daemon API endpoints."""
    async with aiohttp.ClientSession() as session:
        print("🧪 Testing CPU Orchestrator Daemon API...")
        
        # Test root endpoint
        print("\n📋 Testing root endpoint...")
        async with session.get(f"{API_BASE}/") as resp:
            data = await resp.json()
            print(f"✅ Root: {data['name']}")
        
        # Test metrics endpoint
        print("\n📊 Testing metrics endpoint...")
        async with session.get(f"{API_BASE}/metrics") as resp:
            data = await resp.json()
            print(f"✅ GPU Utilization: {data['gpu_utilization']}%")
            print(f"✅ Memory Used: {data['memory_used']:.2f}GB / {data['memory_total']}GB")
            print(f"✅ Temperature: {data['temperature']}°C")
        
        # Test users endpoint
        print("\n👥 Testing users endpoint...")
        async with session.get(f"{API_BASE}/users") as resp:
            users = await resp.json()
            print(f"✅ Found {len(users)} users")
            for user in users:
                print(f"   - {user['username']}: {user['max_concurrent_jobs']} max jobs")
        
        # Test job submission
        print("\n📝 Testing job submission...")
        job_data = {
            "job_id": "test_job_1",
            "cpp_file": "examples/hello_world.cpp",
            "user": "alice",
            "priority": 5
        }
        
        async with session.post(f"{API_BASE}/jobs", json=job_data) as resp:
            if resp.status == 200:
                result = await resp.json()
                print(f"✅ Job submitted: {result['message']}")
            else:
                error = await resp.text()
                print(f"❌ Job submission failed: {error}")
        
        # Wait a bit for job to process
        print("\n⏳ Waiting for job to process...")
        await asyncio.sleep(5)
        
        # Check job status
        print("\n📊 Checking job status...")
        async with session.get(f"{API_BASE}/jobs") as resp:
            jobs = await resp.json()
            print(f"✅ Found {len(jobs)} jobs")
            for job in jobs:
                print(f"   - {job['job_id']}: {job['status']} (priority: {job['priority']})")
        
        # Test system status
        print("\n🖥️  Testing system status...")
        async with session.get(f"{API_BASE}/status") as resp:
            status = await resp.json()
            print(f"✅ Running jobs: {status['scheduler']['running_jobs']}")
            print(f"✅ Queued jobs: {status['scheduler']['queued_jobs']}")
            print(f"✅ Total jobs: {status['job_statistics']['total']}")
        
        # Submit a second job to test queueing
        print("\n📝 Testing job queueing...")
        job_data2 = {
            "job_id": "test_job_2",
            "cpp_file": "examples/math_computation.cpp",
            "user": "bob",
            "priority": 3
        }
        
        async with session.post(f"{API_BASE}/jobs", json=job_data2) as resp:
            if resp.status == 200:
                result = await resp.json()
                print(f"✅ Second job submitted: {result['message']}")
        
        print("\n🎉 API tests completed!")

async def main():
    """Main test function."""
    print("Starting API tests...")
    print("Make sure the daemon is running with: python daemon.py")
    print("Press Ctrl+C to stop tests")
    
    try:
        await asyncio.sleep(2)  # Give time for daemon to start
        await test_api()
    except aiohttp.ClientConnectorError:
        print("❌ Could not connect to daemon. Make sure it's running on localhost:8000")
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")

if __name__ == "__main__":
    asyncio.run(main())