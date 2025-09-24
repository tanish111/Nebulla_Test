#include <iostream>
#include <vector>
#include <numeric>
#include <random>
#include <chrono>

int main() {
    std::cout << "Starting compute-intensive task..." << std::endl;
    
    const int size = 1000000;
    std::vector<double> data(size);
    
    // Fill with random data
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(1.0, 100.0);
    
    for (int i = 0; i < size; ++i) {
        data[i] = dis(gen);
    }
    
    // Compute sum and average
    double sum = std::accumulate(data.begin(), data.end(), 0.0);
    double avg = sum / size;
    
    std::cout << "Processed " << size << " elements" << std::endl;
    std::cout << "Sum: " << sum << std::endl;
    std::cout << "Average: " << avg << std::endl;
    std::cout << "Compute task completed!" << std::endl;
    
    return 0;
}