// Command mqboom is an AMQP load generator.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"runtime"

	"github.com/zgo/mqboom"
)

const (
	headerRegexp = `^([\w-]+):\s*(.+)`
)

type headerSlice []string

func (h *headerSlice) String() string {
	return fmt.Sprintf("%s", *h)
}

func (h *headerSlice) Set(value string) error {
	*h = append(*h, value)
	return nil
}

var (
	headerslice headerSlice
	headers     = flag.String("h", "", "")
	body        = flag.String("d", "test", "")
	contentType = flag.String("T", "text/html", "")

	output = flag.String("o", "", "")

	c = flag.Int("c", 50, "")
	n = flag.Int("n", 200, "")
	q = flag.Int("q", 0, "")
	t = flag.Int("t", 0, "")

	cpus = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")
)

func main() {
	flag.Var(&headerslice, "H", "")

	flag.Parse()
	if flag.NArg() < 1 {
		usageAndExit("")
	}

	runtime.GOMAXPROCS(*cpus)
	num := *n
	conc := *c
	q := *q

	if num <= 0 || conc <= 0 {
		usageAndExit("n and c cannot be smaller than 1.")
	}

	if num < conc {
		usageAndExit("n cannot be less than c")
	}

	url := flag.Args()[0]

	// set content-type
	header := make(http.Header)
	header.Set("Content-Type", *contentType)
	// set any other additional headers
	if *headers != "" {
		usageAndExit("flag '-h' is deprecated, please use '-H' instead.")
	}
	// set any other additional repeatable headers
	for _, h := range headerslice {
		match, err := parseInputWithRegexp(h, headerRegexp)
		if err != nil {
			usageAndExit(err.Error())
		}
		header.Set(match[1], match[2])
	}

	if *output != "csv" && *output != "" {
		usageAndExit("Invalid output type; only csv is supported.")
	}

	(&mqboom.Work{
		URI:     url,
		Body:    []byte(*body),
		N:       num,
		C:       conc,
		Qps:     q,
		Timeout: *t,
		Output:  *output,
	}).Run()
}

func usageAndExit(msg string) {
	if msg != "" {
		fmt.Fprintf(os.Stderr, msg)
		fmt.Fprintf(os.Stderr, "\n\n")
	}
	flag.Usage()
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

func parseInputWithRegexp(input, regx string) ([]string, error) {
	re := regexp.MustCompile(regx)
	matches := re.FindStringSubmatch(input)
	if len(matches) < 1 {
		return nil, fmt.Errorf("could not parse the provided input; input = %v", input)
	}
	return matches, nil
}
