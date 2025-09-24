#include <iostream>
#include <chrono>
#include <thread>

int main() {
    std::cout << "Hello from CPU Orchestrator!" << std::endl;
    std::cout << "Starting computation..." << std::endl;
    
    // Simulate some work
    for (int i = 1; i <= 3; i++) {
        std::cout << "Processing step " << i << "/3" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    std::cout << "Computation completed successfully!" << std::endl;
    return 0;
}
