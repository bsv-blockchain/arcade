package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const testArcadeSpec = `{
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

func mergeAndParse(t *testing.T) map[string]interface{} {
	t.Helper()

	merged, err := mergeOpenAPISpecs(testArcadeSpec, "/chaintracks")
	if err != nil {
		t.Fatalf("Failed to merge specs: %v", err)
	}

	var spec map[string]interface{}
	if err := json.Unmarshal([]byte(merged), &spec); err != nil {
		t.Fatalf("Failed to parse merged spec: %v", err)
	}

	return spec
}

func hasChaintracksPath(paths map[string]interface{}) bool {
	for path := range paths {
		if strings.HasPrefix(path, "/chaintracks") {
			return true
		}
	}

	return false
}

func hasPrefixedChaintracksTag(tags []interface{}) bool {
	for _, tag := range tags {
		tagMap, ok := tag.(map[string]interface{})
		if !ok {
			continue
		}

		name, ok := tagMap["name"].(string)
		if ok && strings.HasPrefix(name, "chaintracks-") {
			return true
		}
	}

	return false
}

func TestMergeOpenAPISpecs(t *testing.T) {
	spec := mergeAndParse(t)

	paths, ok := spec["paths"].(map[string]interface{})
	if !ok {
		t.Fatal("paths not found in merged spec")
	}

	if _, hasTx := paths["/tx"]; !hasTx {
		t.Error("Arcade /tx path not found in merged spec")
	}

	if !hasChaintracksPath(paths) {
		t.Error("No chaintracks paths found with /chaintracks prefix")
	}

	tags, ok := spec["tags"].([]interface{})
	if !ok || len(tags) < 2 {
		t.Errorf("Expected at least 2 tags after merge, got %d", len(tags))
	}

	if !hasPrefixedChaintracksTag(tags) {
		t.Error("No chaintracks- prefixed tags found")
	}

	t.Logf("Successfully merged specs with %d total paths", len(paths))
}
