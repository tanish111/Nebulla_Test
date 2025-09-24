"""
Mock GPU metrics generator for the CPU orchestrator daemon.
"""
import asyncio
import random
from datetime import datetime
from typing import Optional
from models import GPUMetrics

class MockGPUMetricsGenerator:
    def __init__(self):
        self.memory_total = 16.0  # 16GB total GPU memory
        self.current_metrics: Optional[GPUMetrics] = None
        self.base_utilization = 20.0  # Base utilization percentage
        self.base_memory_used = 2.0  # Base memory usage in GB
        self.base_temperature = 45.0  # Base temperature in Celsius
        self.running = False

    def generate_metrics(self) -> GPUMetrics:
        """Generate mock GPU metrics with realistic variations."""
        # Simulate realistic GPU metrics with some randomness
        utilization = max(0, min(100, 
            self.base_utilization + random.uniform(-15, 35)
        ))
        
        # Memory usage correlates somewhat with utilization
        memory_factor = utilization / 100.0
        memory_used = max(0.5, min(self.memory_total,
            self.base_memory_used + (memory_factor * 8) + random.uniform(-1, 2)
        ))
        
        # Temperature correlates with utilization
        temp_factor = utilization / 100.0
        temperature = max(30, min(90,
            self.base_temperature + (temp_factor * 25) + random.uniform(-5, 10)
        ))

        metrics = GPUMetrics(
            gpu_utilization=round(utilization, 1),
            memory_used=round(memory_used, 2),
            memory_total=self.memory_total,
            temperature=round(temperature, 1),
            timestamp=datetime.now()
        )

        self.current_metrics = metrics
        return metrics

    def get_current_metrics(self) -> GPUMetrics:
        """Get the current metrics, generating new ones if none exist."""
        if self.current_metrics is None:
            return self.generate_metrics()
        return self.current_metrics

    async def start_generation(self, interval: float = 3.0, store_callback=None):
        """Start generating metrics at regular intervals."""
        self.running = True
        while self.running:
            metrics = self.generate_metrics()
            
            # Store metrics in database if callback provided
            if store_callback:
                try:
                    await store_callback(metrics)
                except Exception as e:
                    print(f"Error storing metrics: {e}")
            
            # Simulate some job load variations
            if random.random() < 0.1:  # 10% chance to simulate job activity
                self.base_utilization = max(10, min(80, 
                    self.base_utilization + random.uniform(-20, 30)
                ))
                self.base_memory_used = max(1, min(12,
                    self.base_memory_used + random.uniform(-2, 4)
                ))
            
            await asyncio.sleep(interval)

    def stop_generation(self):
        """Stop generating metrics."""
        self.running = False

    def simulate_job_load(self, job_count: int = 0):
        """Simulate increased load based on running jobs."""
        if job_count == 0:
            # Idle state
            self.base_utilization = max(5, self.base_utilization - 2)
            self.base_memory_used = max(1, self.base_memory_used - 0.5)
        else:
            # Increase base metrics based on job count
            self.base_utilization = min(85, self.base_utilization + (job_count * 15))
            self.base_memory_used = min(14, self.base_memory_used + (job_count * 2))

    def simulate_high_priority_job(self):
        """Simulate a spike for high-priority job execution."""
        self.base_utilization = min(95, self.base_utilization + 40)
        self.base_memory_used = min(15, self.base_memory_used + 4)
        self.base_temperature = min(85, self.base_temperature + 15)