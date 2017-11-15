## LeoFS Client TEST

### Preparation
Generate Test Data with
```bash
$ cd temp_data; ./gen.sh
```

Some SDKs (e.g. erlcloud, aws-sdk-php, aws-sdk-cpp), connect to {BUCKETNAME}.{HOST},
you have to add corresponding entries to `/etc/hosts` for name resolving
Reference: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html

Eg: For localhost, add `<testname>.localhost` to the line starting with 127.0.0.1, seperated by space.

### Command Format
```bash
$ tester [SIGNATURE_VERSION] [HOST] [PORT] [BUCKET]
```
Note that some libraries only support v2/v4 signature

### C++ - aws-sdk-cpp
#### Get the library

```bash
$ git submodule update -i
```

#### Execute the test-case

```bash
$ cd aws-sdk-cpp
$ mkdir build
$ cd build
$ cmake -DBUILD_ONLY="s3" ..
$ make
$ LeoFSTest.cpp v4 localhost 8080 testc
```

### GO - aws-sdk-go
#### Install the libraries

```bash
$ sudo apt-get install golang
$ mkdir $HOME/go
$ export GOPATH=$HOME/go
$ go get github.com/aws/aws-sdk-go/service/s3
```

#### Execute the test-case

```bash
$ cd aws-sdk-go
$ go run LeoFSTest.go v4 localhost 8080 testg
```

### Java - aws-sdk-java
#### Execute the test-case

```bash
$ cd aws-sdk-java
$ ant -Dsignver=v4 -Dhost="localhost" -Dport=8080 -Dbucket="testj"
or
$ ant -Dsignver=v2 -Dhost="localhost" -Dport=8080 -Dbucket="testj"
```

### PHP - aws-sdk-php
#### Install the libraries

```bash
$ cd aws-sdk-php
$ curl -sS https://getcomposer.org/installer | php
$ php composer.phar install
```

#### Execute the test-case

```bash
$ cd aws-sdk-php
$ php LeoFSTest.php v4 localhost 8080 testp
or
$ php LeoFSTest.php v2 localhost 8080 testp
```

### Ruby - aws-sdk-ruby
#### Install the libraries

* content_type depends on libmagic

```bash
## CentOS/Fedora/RHEL:
$ sudo yum install libmagic-devel
$ sudo apt-get install ruby-devel

## Ubuntu/Debian
$ sudo apt-get install libmagic-dev
$ sudo apt-get install ruby-dev
```

* Install the gems

```bash
$ sudo gem install aws-sdk
$ sudo gem install content_type
```

#### Execute the test-case

```bash
$ cd aws-sdk-ruby
$ ruby LeoFSTest.rb v4 localhost 8080 testr
or
$ ruby LeoFSTest.rb v2 localhost 8080 testr
```

### Python - boto
#### Install the libraries

```bash
$ sudo pip install boto
$ sudo pip install filechunkio
```

#### Execute the test-case

```bash
$ python LeoFSTest.py v4 localhost 8080 testb
or
$ python LeoFSTest.py v2 localhost 8080 testb
```

### Python - boto3
#### Install the libraries

```bash
$ sudo pip install boto3
$ sudo pip install filechunkio
```

#### Execute the test-case

```bash
$ python LeoFSTest.py v4 localhost 8080 testb3
or
$ python LeoFSTest.py v2 localhost 8080 testb3
```

### Erlang - erlcloud
#### Install the libraries
```bash
$ cd erlcloud
$ make
```

#### Execute the test-case
```bash
$ ./LeoFSTest.erl v2 localhost 8080 teste
```

#### Java - jclouds
#### Install the libraries
```bash
$ cd jclouds
$ mvn dependency:copy-dependencies
```

#### Execute the test-case
```bash
$ ant -Dsignver=v2 -Dhost="localhost" -Dport=8080 -Dbucket="testj"
```
