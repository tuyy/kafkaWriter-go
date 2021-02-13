## toy-project-go 003
 go v1.15.3

### kafkaWriter-go
 file을 읽어서 kafka에 produce 한다.

```
$ make build
$ cd dist;./kafkaWriter -h

Usage of ./kafkaWriter:
  -b string
        broker server list(delim:',')
  -f string
        input filename (default "input.log")
  -t string
        topic name for writing

## ex)
$ ./kafkaWriter -b=dev-tuyy0-cassandra001-ncl.nfra.io:9092 -t=mytest1 -f=input.log
```
