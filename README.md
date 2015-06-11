## LeoFS Client TEST

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
$ sudo gem install aws-sdk-v1
$ sudo gem install content_type
```

#### Execute the test-case

```bash
$ cd aws-sdk-ruby
$ ruby leo.rb
$ ruby leo2.rb
``` 

### Python - boto
#### Install the libraries

```bash
$ sudo pip install boto
$ sudo pip install python-magic
$ sudo pip install filechunkio
```

#### Execute the test-case

```bash
$ python leo.py
```

### Java - aws-sdk-java
#### Execute the test-case

```bash
$ cd aws-sdk-java
$ ant
```

### PHP - aws-sdk-php
#### Install the libraries

```bash
$ curl -sS https://getcomposer.org/installer | php
$ php composer.phar install
```

#### Execute the test-case

```bash
$ cd aws-sdk-php
$ php index.php
```

### Erlang - erlcloud
#### Install the libraries

#### Execute the test-case

```bash
$ make 
```
### s3cmd
