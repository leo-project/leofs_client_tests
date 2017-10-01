#include <iostream>

#include <aws/core/Aws.h>

int main()
{
    Aws::SDKOptions options;
    // set the options
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    // end of options
    std::cout << "=== AWS API Init: Start===\n";
    Aws::InitAPI(options);
    std::cout << "=== AWS API Init: Success ===\n";
    // call tests here

    // end of tests
    std::cout << "=== AWS API Shutdown: Start===\n";
    Aws::ShutdownAPI(options);
    std::cout << "=== AWS API Shutdown: Success ===\n";
    return 0;
}
