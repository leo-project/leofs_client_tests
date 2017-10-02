#include <iostream>
#include <string>

#include <aws/core/Aws.h>

#include "definitions.h"

int main(int argc, char** argv)
{
    Aws::SDKOptions options;
    // set the options
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    // end of options
    std::cout << "=== AWS API Init: Start===\n";
    Aws::InitAPI(options);
    std::cout << "=== AWS API Init: Success ===\n";
    // setup
    std::string signVer = SIGN_VER, host = HOST, portStr = PORT, bucket = BUCKET;
    if (argc)
    {
        signVer = argv[1];
        host = argv[2];
        portStr = argv[3];
        bucket = argv[4];
    }
    int port = std::stoi(portStr);

    Aws::ClientConfiguration config;
    config.region = "us-west-2";
    config.endpointOverride = "http://" + host + ":" + port;
    config.scheme = Aws::Http::Scheme::HTTP;
    Aws::Auth::AWSCredentials cred(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    Aws::S3::S3Client client(cred, config);
    // call tests here

    std::cout << SMALL_TEST_FILE << std::endl;

    // end of tests
    std::cout << "=== AWS API Shutdown: Start===\n";
    Aws::ShutdownAPI(options);
    std::cout << "=== AWS API Shutdown: Success ===\n";
    return 0;
}
