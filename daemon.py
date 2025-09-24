#!/usr/bin/env python3
"""
CPU Orchestrator Daemon - Main entry point.

A CPU-only prototype of a GPU orchestration daemon inspired by Run:AI,
designed for job scheduling, resource tracking, and monitoring.
"""
import asyncio
import signal
import sys
import logging
import uvicorn
from contextlib import asynccontextmanager

from api import app
from database import DatabaseManager
from scheduler import JobScheduler
from metrics_generator import MockGPUMetricsGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cpu_orchestrator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CPUOrchestratorDaemon:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.metrics_generator = MockGPUMetricsGenerator()
        self.scheduler = JobScheduler(self.db_manager, max_concurrent_jobs=2)
        self.server = None
        self.running = False

    async def initialize(self):
        """Initialize all components."""
        logger.info("Initializing CPU Orchestrator Daemon...")
        
        # Initialize database
        await self.db_manager.init_db()
        logger.info("Database initialized")
        
        # Create some default users for testing
        await self.create_default_users()
        
        logger.info("Daemon initialization complete")

    async def create_default_users(self):
        """Create default users for testing."""
        default_users = [
            {"username": "alice", "max_jobs": 5, "max_cores": 4, "max_memory": 8.0},
            {"username": "bob", "max_jobs": 3, "max_cores": 2, "max_memory": 4.0},
            {"username": "charlie", "max_jobs": 2, "max_cores": 1, "max_memory": 2.0}
        ]
        
        for user_data in default_users:
            await self.db_manager.create_user_if_not_exists(
                user_data["username"],
                user_data["max_jobs"],
                user_data["max_cores"],
                user_data["max_memory"]
            )
            logger.info(f"Default user {user_data['username']} created/verified")

    async def start_background_tasks(self):
        """Start background tasks."""
        logger.info("Starting background tasks...")
        
        # Start metrics generation
        asyncio.create_task(
            self.metrics_generator.start_generation(
                interval=3.0,
                store_callback=self.db_manager.store_metrics
            )
        )
        logger.info("Metrics generation started")
        
        # Start job scheduler
        asyncio.create_task(self.scheduler.start_scheduler())
        logger.info("Job scheduler started")

    async def start_api_server(self, host: str = "0.0.0.0", port: int = 8000):
        """Start the FastAPI server."""
        logger.info(f"Starting API server on {host}:{port}")
        
        config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="info",
            access_log=True
        )
        
        self.server = uvicorn.Server(config)
        await self.server.serve()

    async def stop(self):
        """Stop all components gracefully."""
        logger.info("Stopping CPU Orchestrator Daemon...")
        
        self.running = False
        
        # Stop metrics generation
        self.metrics_generator.stop_generation()
        logger.info("Metrics generation stopped")
        
        # Stop scheduler
        self.scheduler.stop_scheduler()
        logger.info("Job scheduler stopped")
        
        # Stop API server
        if self.server:
            self.server.should_exit = True
            logger.info("API server stopped")
        
        logger.info("Daemon stopped successfully")

    async def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the daemon."""
        try:
            self.running = True
            
            # Initialize components
            await self.initialize()
            
            # Start background tasks
            await self.start_background_tasks()
            
            # Display startup information
            self.display_startup_info(host, port)
            
            # Start API server (this blocks until server stops)
            await self.start_api_server(host, port)
            
        except Exception as e:
            logger.error(f"Error running daemon: {e}")
            await self.stop()
            raise

    def display_startup_info(self, host: str, port: int):
        """Display startup information."""
        print("\n" + "="*60)
        print("🚀 CPU Orchestrator Daemon Started Successfully!")
        print("="*60)
        print(f"📡 API Server: http://{host}:{port}")
        print(f"📊 Metrics: http://{host}:{port}/metrics")
        print(f"📋 Jobs: http://{host}:{port}/jobs")
        print(f"👥 Users: http://{host}:{port}/users")
        print(f"📈 Status: http://{host}:{port}/status")
        print("="*60)
        print("📝 Example Usage:")
        print(f"   curl http://{host}:{port}/metrics")
        print(f"   curl -X POST http://{host}:{port}/jobs \\")
        print(f"        -H 'Content-Type: application/json' \\")
        print(f"        -d '{{\"job_id\": \"test1\", \"cpp_file\": \"test.cpp\", \"user\": \"alice\", \"priority\": 1}}'")
        print("="*60)
        print("Press Ctrl+C to stop the daemon")
        print("="*60 + "\n")

async def main():
    """Main function."""
    daemon = CPUOrchestratorDaemon()
    
    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        asyncio.create_task(daemon.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await daemon.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        await daemon.stop()
    except Exception as e:
        logger.error(f"Daemon error: {e}")
        await daemon.stop()
        sys.exit(1)

if __name__ == "__main__":
    # Check if help is requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("""
CPU Orchestrator Daemon - A CPU-only prototype of a GPU orchestration daemon

Usage:
    python daemon.py [options]

Options:
    -h, --help    Show this help message
    
Environment variables:
    HOST          Server host (default: 0.0.0.0)
    PORT          Server port (default: 8000)

Features:
    • Mock GPU metrics generation
    • Job scheduling with priority and preemption
    • User quota management
    • REST API endpoints
    • SQLite persistence
    • C++ job compilation and execution

API Endpoints:
    GET  /metrics              - Get current GPU metrics
    GET  /jobs                 - List all jobs
    POST /jobs                 - Submit a new job
    GET  /jobs/{id}           - Get specific job
    POST /jobs/{id}/cancel    - Cancel a job
    POST /schedule            - Trigger manual scheduling
    GET  /users               - List all users
    POST /users               - Create a new user
    GET  /status              - Get system status
        """)
        sys.exit(0)
    
    # Run the daemon
    asyncio.run(main())