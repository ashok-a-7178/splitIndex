package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// IndexGenerator generates a test Bleve index with configurable documents.
type IndexGenerator struct {
	cfg        *Config
	dictionary []string
}

// NewIndexGenerator creates a new IndexGenerator with the given config.
func NewIndexGenerator(cfg *Config) *IndexGenerator {
	return &IndexGenerator{
		cfg:        cfg,
		dictionary: defaultDictionary(),
	}
}

// Generate creates the source index at cfg.SourceIndexDir.
// Returns the elapsed time in milliseconds.
func (g *IndexGenerator) Generate() (int64, error) {
	// Check if index already exists
	if info, err := bleve.Open(g.cfg.SourceIndexDir); err == nil {
		_ = info.Close()
		fmt.Printf("[IndexGenerator] Source index already exists at: %s\n", g.cfg.SourceIndexDir)
		fmt.Println("[IndexGenerator] Skipping generation. Delete the directory to regenerate.")
		return 0, nil
	}

	fmt.Printf("[IndexGenerator] Generating index with %d docs, %d fields, %d unique IDs\n",
		g.cfg.TotalDocs, g.cfg.NumFields, g.cfg.NumUniqueIDs)
	fmt.Printf("[IndexGenerator] Target doc size: ~%d bytes\n", g.cfg.TargetDocSizeBytes)
	fmt.Printf("[IndexGenerator] Target directory: %s\n", g.cfg.SourceIndexDir)

	start := time.Now()

	// Create index mapping
	indexMapping := g.createIndexMapping()

	// Create the index
	index, err := bleve.New(g.cfg.SourceIndexDir, indexMapping)
	if err != nil {
		return 0, fmt.Errorf("create index: %w", err)
	}
	defer index.Close()

	// Index documents in batches
	rng := rand.New(rand.NewSource(42))
	batch := index.NewBatch()
	batchCount := 0

	for i := 0; i < g.cfg.TotalDocs; i++ {
		doc := g.createDocument(i, rng)
		docID := fmt.Sprintf("doc_%d", i)
		if err := batch.Index(docID, doc); err != nil {
			return 0, fmt.Errorf("batch index doc %d: %w", i, err)
		}
		batchCount++

		if batchCount >= g.cfg.IndexBatchSize {
			if err := index.Batch(batch); err != nil {
				return 0, fmt.Errorf("commit batch at doc %d: %w", i, err)
			}
			batch = index.NewBatch()
			batchCount = 0

			if (i+1)%10_000 == 0 {
				fmt.Printf("[IndexGenerator] Indexed %d / %d docs\n", i+1, g.cfg.TotalDocs)
			}
		}
	}

	// Flush remaining batch
	if batchCount > 0 {
		if err := index.Batch(batch); err != nil {
			return 0, fmt.Errorf("commit final batch: %w", err)
		}
	}

	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("[IndexGenerator] All %d docs indexed\n", g.cfg.TotalDocs)
	fmt.Printf("[IndexGenerator] Index generation completed in %d ms\n", elapsed)
	fmt.Printf("[IndexGenerator] Index size: %.2f MB\n", dirSizeMB(g.cfg.SourceIndexDir))
	return elapsed, nil
}

// createIndexMapping creates a Bleve index mapping with text and keyword fields.
func (g *IndexGenerator) createIndexMapping() mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()

	docMapping := bleve.NewDocumentMapping()

	// ID field — keyword (not analyzed)
	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("ID", keywordFieldMapping)

	// docSequence — numeric
	numFieldMapping := bleve.NewNumericFieldMapping()
	numFieldMapping.Store = true
	docMapping.AddFieldMappingsAt("docSequence", numFieldMapping)

	// Dynamic fields — text and keyword alternating
	for f := 2; f < g.cfg.NumFields; f++ {
		fieldName := fmt.Sprintf("field_%d", f)
		switch f % 5 {
		case 0:
			kfm := bleve.NewKeywordFieldMapping()
			kfm.Store = true
			docMapping.AddFieldMappingsAt(fieldName, kfm)
		case 1:
			tfm := bleve.NewTextFieldMapping()
			tfm.Store = true
			docMapping.AddFieldMappingsAt(fieldName, tfm)
		case 2, 3, 4:
			nfm := bleve.NewNumericFieldMapping()
			nfm.Store = true
			docMapping.AddFieldMappingsAt(fieldName, nfm)
		}
	}

	indexMapping.DefaultMapping = docMapping
	return indexMapping
}

// createDocument creates a single document map with the configured fields.
func (g *IndexGenerator) createDocument(docIndex int, rng *rand.Rand) map[string]interface{} {
	doc := make(map[string]interface{})

	idValue := fmt.Sprintf("ID_%d", docIndex%g.cfg.NumUniqueIDs)
	doc["ID"] = idValue
	doc["docSequence"] = docIndex

	// Calculate text budget
	fixedBytes := len(idValue) + 4 // ID + docSequence
	numTextFields := 0
	for f := 2; f < g.cfg.NumFields; f++ {
		switch f % 5 {
		case 0:
			numTextFields++
		case 1:
			numTextFields++
		case 2, 3, 4:
			fixedBytes += 8
		}
	}
	textBudget := g.cfg.TargetDocSizeBytes - fixedBytes
	bytesPerTextField := 8
	if numTextFields > 0 {
		bytesPerTextField = max(8, textBudget/numTextFields)
	}

	for f := 2; f < g.cfg.NumFields; f++ {
		fieldName := fmt.Sprintf("field_%d", f)
		switch f % 5 {
		case 0:
			doc[fieldName] = g.dictionaryWord(rng)
		case 1:
			doc[fieldName] = g.dictionaryPhrase(bytesPerTextField, rng)
		case 2:
			doc[fieldName] = rng.Intn(100_000)
		case 3:
			doc[fieldName] = rng.Int63()
		case 4:
			doc[fieldName] = rng.Float64() * 1000
		}
	}

	return doc
}

// dictionaryWord picks a random word from the dictionary.
func (g *IndexGenerator) dictionaryWord(rng *rand.Rand) string {
	return g.dictionary[rng.Intn(len(g.dictionary))]
}

// dictionaryPhrase builds a phrase of approximately targetBytes length.
func (g *IndexGenerator) dictionaryPhrase(targetBytes int, rng *rand.Rand) string {
	var sb strings.Builder
	sb.Grow(targetBytes)

	for sb.Len() < targetBytes {
		word := g.dictionary[rng.Intn(len(g.dictionary))]
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(word)
	}

	result := sb.String()
	if len(result) > targetBytes {
		result = result[:targetBytes]
	}
	return result
}

// defaultDictionary returns a large vocabulary for document generation.
func defaultDictionary() []string {
	return []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
		"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
		"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
		"yankee", "zulu", "abstract", "boolean", "class", "double", "extends",
		"final", "generic", "hashmap", "integer", "javadoc", "keyword", "lambda",
		"module", "native", "object", "package", "qualifier", "return", "static",
		"thread", "utility", "volatile", "wrapper", "xerces", "yield", "zenith",
		"algorithm", "benchmark", "compiler", "database", "endpoint", "framework",
		"gateway", "hibernate", "iterator", "jackson", "kubernetes", "lifecycle",
		"middleware", "namespace", "orchestrator", "pipeline", "queryable", "resolver",
		"scheduler", "transformer", "upstream", "validator", "webservice", "xmlparser",
		"yesterday", "zookeeper", "analytics", "bootstrap", "container", "deployment",
		"elasticsearch", "federation", "graphql", "horizontal", "ingestion", "jmeter",
		"loadbalancer", "microservice", "notification", "observability",
		"prometheus", "quarantine", "replication", "serverless", "telemetry",
		"vectorized", "workload", "accumulator", "bytebuffer", "checkpoint",
		"dispatcher", "encryption", "filesystem", "governance", "heartbeat",
		"idempotent", "journaling", "keystore", "linearizable", "monotonic",
		"optimistic", "partitioner", "quorum", "serialization", "timestamp",
		"uncommitted", "versionable", "annotation", "backtrace", "coroutine",
		"deserialize", "enumerable", "finalizer", "generational", "heapdump",
		"interleaved", "joinpoint", "lockfree", "memtable", "nullsafe",
		"overloaded", "polymorphic", "quotient", "recursive", "semaphore",
		"tombstone", "underflow", "viewport", "watermark",
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
