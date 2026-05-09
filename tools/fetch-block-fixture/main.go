// Command fetch-block-fixture downloads a real BSV mainnet block and
// reconstructs the test fixtures the e2e harness needs to drive a
// merkle-proof return-path scenario:
//
//   - block.bin    — the binary block the harness datahub serves at
//     /block/<hash>. Identical to teranode's served
//     bytes except the trailing coinbase-BUMP slot
//     is replaced with a real coinbase merkle path
//     so arcade's bump-builder validates against the
//     real header merkle root.
//   - subtrees/<hash>.bin — concatenated 32-byte tx hashes, with the
//     coinbase placeholder at index 0 of subtree[0]
//     (matches teranode's subtree.CoinbasePlaceholder
//     convention).
//   - txs/<txid>.bin     — raw tx body for each picked watch target.
//   - meta.json          — height, root, txCount, picked txids,
//     provenance (datahub URL, woc URL, tool version).
//
// Usage:
//
//	go run ./tools/fetch-block-fixture \
//	    --block 000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396 \
//	    --out tests/e2e/fixtures/blocks/000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396
//
// The tool is idempotent for fixed --seed: a given (block, seed) always
// produces identical bytes. Defaults are picked so a re-run with no
// flags reproduces the bytes a CI peer would have generated.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
)

const (
	defaultDatahubURL = "http://bsva-ovh-teranode-eu-1.bsvb.tech:8000/api/v1"
	defaultWOCURL     = "https://api.whatsonchain.com"
	defaultPickN      = 10
	defaultSeed       = 1
)

func main() {
	blockHash := flag.String("block", "", "block hash (64 hex chars) to fetch")
	outDir := flag.String("out", "", "output directory (will be created)")
	datahubURL := flag.String("datahub", defaultDatahubURL, "teranode datahub base URL (serves /block/<hash>)")
	wocURL := flag.String("woc", defaultWOCURL, "WhatsOnChain base URL (serves /v1/bsv/main/...)")
	pickN := flag.Int("pick", defaultPickN, "number of non-coinbase txids to pick for the fixture")
	seed := flag.Int64("seed", defaultSeed, "RNG seed for txid pick (reproducibility)")
	flag.Parse()

	if *blockHash == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "fetch-block-fixture: --block and --out are required")
		flag.Usage()
		os.Exit(2)
	}
	if *pickN <= 0 {
		fmt.Fprintln(os.Stderr, "fetch-block-fixture: --pick must be > 0")
		os.Exit(2)
	}

	cfg := config{
		blockHash:  *blockHash,
		outDir:     *outDir,
		datahubURL: *datahubURL,
		wocURL:     *wocURL,
		pickN:      *pickN,
		rng:        rand.New(rand.NewPCG(uint64(*seed), uint64(*seed))), //nolint:gosec // deterministic by design
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fetch-block-fixture: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	blockHash  string
	outDir     string
	datahubURL string
	wocURL     string
	pickN      int
	rng        *rand.Rand
}
