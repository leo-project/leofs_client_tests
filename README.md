## LeoFS Client TEST

### Ruby - aws-sdk-ruby
#### Install the libraries

* content_type depends on libmagic

```bash
## Ubuntu/Debian
$ sudo yum install libmagic-devel

## CentOS/Fedora/RHEL: 
$ sudo apt-get install libmagic-dev
```

* Install the gems

```bash
$ gem install aws-sdk
$ gem install content_type
```

#### Execute the test-case

```bash
$ cd aws-sdk-ruby
$ ruby leo.rb
``` 

### Python - boto
#### Install the libraries

```bash
$ sudo pip install boto
$ sudo pip install python-magic
```

#### Execute the test-case

```bash
$ python leo.py
```

### Java - aws-sdk-java

### PHP - aws-sdk-php

### Erlang - erlcloud

### s3cmd
