package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `mk — mini-kafka CLI

Usage:
  mk [--broker host:port] <command> [flags] [arguments]

Global flags:
  --broker   Broker address (default: localhost:9092)

Commands:
  topics list
  topics create [--partitions N] [--replication-factor N] <topic>
  topics describe <topic>

  produce [--key K] [--value V] [--count N] [--partition N] <topic>
  consume [--from-beginning] [--partition N] [--group G] <topic>

  groups describe <group-id>

Examples:
  mk topics list
  mk topics create --partitions 3 orders
  mk topics describe orders

  mk produce --key user-1 --value "hello" orders
  mk produce --count 10 --value "ping" orders
  mk produce orders                                # reads from stdin

  mk consume --from-beginning orders
  mk consume --partition 0 --group my-app orders

  mk groups describe my-app

Note: flags must come before the topic/group argument.
`

func main() {
	broker := flag.String("broker", "localhost:9092", "Broker address (host:port)")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	var err error
	switch args[0] {
	case "topics":
		err = cmdTopics(args[1:], *broker)
	case "produce":
		err = cmdProduce(args[1:], *broker)
	case "consume":
		err = cmdConsume(args[1:], *broker)
	case "groups":
		err = cmdGroups(args[1:], *broker)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", args[0])
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func newFlagSet(cmd string) *flag.FlagSet {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", cmd)
		fs.PrintDefaults()
	}
	return fs
}
