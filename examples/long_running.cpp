#include <iostream>
#include <chrono>
#include <thread>

int main() {
    std::cout << "Starting long-running job..." << std::endl;
    
    // Simulate a long-running task
    for (int i = 1; i <= 5; i++) {
        std::cout << "Long computation step " << i << "/5" << std::endl;
        
        // Do some work
        long long sum = 0;
        for (int j = 0; j < 10000000; j++) {
            sum += j % 1000;
        }
        
        std::cout << "Intermediate result: " << sum << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    
    std::cout << "Long-running job completed!" << std::endl;
    return 0;
}
