package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	cfg := DefaultConfig()
	cfg.Print()

	mode := "all"
	if len(os.Args) > 1 {
		mode = strings.ToLower(os.Args[1])
	}

	switch mode {
	case "generate":
		runGenerate(cfg)
	case "copy":
		runCopySplit(cfg)
	case "hardlink":
		runHardLinkSplit(cfg)
	case "smoketest":
		runSmokeTest(cfg)
	case "clean":
		runClean(cfg)
	case "all":
		runFullBenchmark(cfg)
	default:
		fmt.Printf("Unknown mode: %s\n", mode)
		fmt.Println("Usage: go-benchmark [generate|copy|hardlink|smoketest|clean|all]")
		os.Exit(1)
	}
}

func runFullBenchmark(cfg *Config) {
	// Step 1: Generate index
	genTime, err := runGenerate(cfg)
	if err != nil {
		fmt.Printf("FATAL: %v\n", err)
		os.Exit(1)
	}

	// Step 2: Run copy-based split
	copyResults, err := runCopySplit(cfg)
	if err != nil {
		fmt.Printf("FATAL: %v\n", err)
		os.Exit(1)
	}

	// Step 3: Clean copy split output
	cleanSplitOutputs(cfg, "copy")

	// Step 4: Run hard-link-based split
	hardlinkResults, err := runHardLinkSplit(cfg)
	if err != nil {
		fmt.Printf("FATAL: %v\n", err)
		os.Exit(1)
	}

	// Step 5: Print comparison
	printComparison(genTime, copyResults, hardlinkResults)

	fmt.Println("\n[Benchmark] To clean up all data, run: go-benchmark clean")
}

func runGenerate(cfg *Config) (int64, error) {
	fmt.Println("\n>>> STEP: Generate Source Index")
	gen := NewIndexGenerator(cfg)
	return gen.Generate()
}

func runCopySplit(cfg *Config) ([3]int64, error) {
	fmt.Println("\n>>> STEP: Run Copy-Based Split")
	splitter := NewCopySplitter(cfg)
	return splitter.Split()
}

func runHardLinkSplit(cfg *Config) ([3]int64, error) {
	fmt.Println("\n>>> STEP: Run Hard-Link-Based Split")
	splitter := NewHardLinkSplitter(cfg)
	return splitter.Split()
}

func runSmokeTest(cfg *Config) {
	test := NewConcurrentCopySmokeTest(cfg.BaseDir)
	if err := test.Run(); err != nil {
		fmt.Printf("Smoke test failed: %v\n", err)
		os.Exit(1)
	}
}

func runClean(cfg *Config) {
	fmt.Println("\n>>> STEP: Clean All Benchmark Data")
	if err := deleteDirectory(cfg.BaseDir); err != nil {
		fmt.Printf("[Clean] Error: %v\n", err)
	} else {
		fmt.Printf("[Clean] Deleted: %s\n", cfg.BaseDir)
	}
}

func cleanSplitOutputs(cfg *Config, splitType string) {
	prefix := cfg.CopySplitDirPrefix
	if splitType == "hardlink" {
		prefix = cfg.HardLinkSplitDirPrefix
	}
	for i := 0; i < cfg.NumSplits; i++ {
		dir := fmt.Sprintf("%s%d", prefix, i)
		_ = deleteDirectory(dir)
	}
	fmt.Printf("[Benchmark] Cleaned %s split outputs\n", splitType)
}

func printComparison(genTime int64, copyResults, hardlinkResults [3]int64) {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("  BENCHMARK RESULTS COMPARISON")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Printf("  Index Generation Time:  %d ms\n", genTime)
	fmt.Println()
	fmt.Println("  +-------------------------------------------------+")
	fmt.Println("  |                 | Copy+Delete | HardLink+Delete  |")
	fmt.Println("  +-------------------------------------------------+")
	fmt.Printf("  | Total Time      | %10d ms | %10d ms    |\n", copyResults[0], hardlinkResults[0])
	fmt.Printf("  | Copy/Link Phase | %10d ms | %10d ms    |\n", copyResults[1], hardlinkResults[1])
	fmt.Printf("  | Delete Phase    | %10d ms | %10d ms    |\n", copyResults[2], hardlinkResults[2])
	fmt.Println("  +-------------------------------------------------+")
	fmt.Println()

	speedup := float64(copyResults[0]) / float64(max(1, int(hardlinkResults[0])))
	copyLinkSpeedup := float64(copyResults[1]) / float64(max(1, int(hardlinkResults[1])))
	fmt.Printf("  Overall Speedup (HardLink vs Copy): %.2fx faster\n", speedup)
	fmt.Printf("  Copy/Link Phase Speedup:            %.2fx faster\n", copyLinkSpeedup)
	fmt.Println()

	if speedup > 1.0 {
		fmt.Println("  >> WINNER: Hard Link approach is faster overall")
	} else if speedup < 1.0 {
		fmt.Println("  >> WINNER: Copy approach is faster overall (unexpected)")
	} else {
		fmt.Println("  >> TIE: Both approaches took the same time")
	}

	fmt.Println()
	fmt.Println("  Notes:")
	fmt.Println("  - The copy/link phase difference is most significant for large indices.")
	fmt.Println("  - Hard link approach also saves disk space during the split process.")
	fmt.Println("  - Delete phase timings should be similar since both do the same operations.")
	fmt.Println("  - Source and target directories must be on the same filesystem for hard links.")
	fmt.Println("============================================================")
}
