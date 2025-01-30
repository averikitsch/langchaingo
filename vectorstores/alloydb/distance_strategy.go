package alloydb

import (
	"errors"
	"fmt"
)

// defaultDistanceStrategy is the default strategy used if none is provided
var defaultDistanceStrategy = CosineDistance{}

type distanceStrategy interface {
	String() string
	operator() string
	searchFunction() string
}

type Euclidean struct{}

func (e Euclidean) String() string {
	return "euclidean"
}
func (e Euclidean) operator() string {
	return "<->"
}
func (e Euclidean) searchFunction() string {
	return "vector_l2_ops"
}

type CosineDistance struct{}

func (c CosineDistance) String() string {
	return "cosineDistance"
}
func (c CosineDistance) operator() string {
	return "<=>"
}
func (c CosineDistance) searchFunction() string {
	return "vector_cosine_ops"
}

type InnerProduct struct{}

func (i InnerProduct) String() string {
	return "innerProduct"
}
func (i InnerProduct) operator() string {
	return "<#>"
}
func (i InnerProduct) searchFunction() string {
	return "vector_ip_ops"
}

// hnswOptions holds the configuration for the hnsw index.
type hnswOptions struct {
	m              int
	efConstruction int
}

// ivfflatOptions holds the configuration for the ivfflat index.
type ivfflatOptions struct {
	lists int
}

// ivfOptions holds the configuration for the ivf index.
type ivfOptions struct {
	lists     int
	quantizer string
}

// scannOptions holds the configuration for the ScaNN index.
type scannOptions struct {
	numLeaves int
	quantizer string
}

// indexOptions returns the specific options for the index based on the index type
func (index *BaseIndex) indexOptions() (string, error) {
	switch index.indexType {
	case "hnsw":
		opts, ok := index.options.(hnswOptions)
		if !ok {
			return "", errors.New("invalid HNSW options")
		}
		return fmt.Sprintf("(m = %d, ef_construction = %d)", opts.m, opts.efConstruction), nil

	case "ivfflat":
		opts, ok := index.options.(ivfflatOptions)
		if !ok {
			return "", errors.New("invalid IVFFlat options")
		}
		return fmt.Sprintf("(lists = %d)", opts.lists), nil

	case "ivf":
		opts, ok := index.options.(ivfOptions)
		if !ok {
			return "", errors.New("invalid IVF options")
		}
		return fmt.Sprintf("(lists = %d, quantizer = %s)", opts.lists, opts.quantizer), nil

	case "ScaNN":
		opts, ok := index.options.(scannOptions)
		if !ok {
			return "", errors.New("invalid ScaNN options")
		}
		return fmt.Sprintf("(num_leaves = %d, quantizer = %s)", opts.numLeaves, opts.quantizer), nil

	default:
		return "", fmt.Errorf("invalid index options of type: %s and options: %v", index.indexType, index.options)
	}
}
