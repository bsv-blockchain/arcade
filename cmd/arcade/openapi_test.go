package main

import (
	"encoding/json"
	"testing"
)

func TestMergeOpenAPISpecs(t *testing.T) {
	// Sample arcade spec (minimal)
	arcadeSpec := `{
		"swagger": "2.0",
		"info": {
			"title": "Arcade API",
			"version": "0.1.0"
		},
		"paths": {
			"/tx": {
				"post": {
					"tags": ["arcade"],
					"summary": "Submit transaction"
				}
			}
		},
		"tags": [
			{"name": "arcade", "description": "Arcade endpoints"}
		]
	}`

	// Merge with chaintracks
	merged, err := mergeOpenAPISpecs(arcadeSpec, "/chaintracks")
	if err != nil {
		t.Fatalf("Failed to merge specs: %v", err)
	}

	// Parse merged spec
	var spec map[string]interface{}
	if err := json.Unmarshal([]byte(merged), &spec); err != nil {
		t.Fatalf("Failed to parse merged spec: %v", err)
	}

	// Verify arcade paths still exist
	paths := spec["paths"].(map[string]interface{})
	if _, ok := paths["/tx"]; !ok {
		t.Error("Arcade /tx path not found in merged spec")
	}

	// Verify chaintracks paths were added with prefix
	foundChaintracksPaths := false
	for path := range paths {
		if len(path) >= 12 && path[:12] == "/chaintracks" {
			foundChaintracksPaths = true
			break
		}
	}
	if !foundChaintracksPaths {
		t.Error("No chaintracks paths found with /chaintracks prefix")
	}

	// Verify tags were merged
	tags := spec["tags"].([]interface{})
	if len(tags) < 2 {
		t.Error("Expected at least 2 tags after merge")
	}

	// Verify chaintracks tags were prefixed
	foundPrefixedTag := false
	for _, tag := range tags {
		if tagMap, ok := tag.(map[string]interface{}); ok {
			if name, ok := tagMap["name"].(string); ok {
				if len(name) >= 12 && name[:12] == "chaintracks-" {
					foundPrefixedTag = true
					break
				}
			}
		}
	}
	if !foundPrefixedTag {
		t.Error("No chaintracks- prefixed tags found")
	}

	t.Logf("Successfully merged specs with %d total paths", len(paths))
}
