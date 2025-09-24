#include <iostream>

int main() {
    std::cout << "This program will demonstrate error handling..." << std::endl;
    
    // Intentional error - division by zero
    int a = 10;
    int b = 0;
    
    std::cout << "Result: " << (a / b) << std::endl;  // This might cause issues
    
    return 1;  // Return non-zero exit code to indicate failure
}