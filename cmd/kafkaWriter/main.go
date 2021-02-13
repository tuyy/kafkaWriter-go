package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/tuyy/kafkaWriter-go/pkg/kafka"
	"log"
	"os"
	"strings"
)

type Args struct {
	Brokers []string
	Topic string
	InputFileName string
}

var args Args

func Init() {
	flag.StringVar(&args.InputFileName, "f", "input.log", "input filename")
	flag.StringVar(&args.Topic, "t", "", "topic name for writing")
	b := flag.String("b", "", "broker server list(delim:',')")
	flag.Parse()

	if args.Topic == "" {
		log.Fatalln("invalid topic.")
	}
	if *b == "" {
		log.Fatalln("invalid broker servers.")
	}
	args.Brokers = strings.Split(*b, ",")
}

func main() {
	Init()

	fmt.Printf("START -- %s, %s, %v\n", args.InputFileName, args.Topic, args.Brokers)

	p := kafka.NewProducer(args.Topic, args.Brokers...)
	defer p.FlushAndClose()

	f, err := os.Open(args.InputFileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)

	for sc.Scan() {
		msg := strings.TrimSpace(sc.Text())
		fmt.Println(msg)
		err = p.WriteMsg(nil, []byte(msg), nil)
		if err != nil {
			log.Fatalln(err)
		}
	}
	if err = sc.Err(); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("FIN --")
}

