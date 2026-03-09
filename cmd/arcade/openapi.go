package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed static/chaintracks-openapi.yaml
var chaintracksOpenAPIYAML []byte

const schemaRefPrefix = "#/components/schemas/"

// OpenAPI Spec Merging
//
// This file implements runtime merging of Arcade's OpenAPI spec with the
// Chaintracks OpenAPI spec. When the chaintracks HTTP API is enabled, both
// specs are combined into a single document served at /docs/openapi.json.
//
// The merge process:
// 1. Loads Arcade's spec from swaggo-generated docs
// 2. Loads Chaintracks spec from embedded YAML file
// 3. Prefixes all chaintracks paths with "/chaintracks"
// 4. Prefixes chaintracks tags with "chaintracks-" to avoid conflicts
// 5. Prefixes chaintracks schema names with "Chaintracks" prefix
// 6. Updates all schema $ref references to use the new prefixed names
//
// This ensures both APIs are documented in a single Scalar UI interface.

// mergeOpenAPISpecs merges the arcade and chaintracks OpenAPI specifications.
// It prefixes all chaintracks paths with the given pathPrefix.
func mergeOpenAPISpecs(arcadeJSON, pathPrefix string) (string, error) {
	arcadeSpec, chaintracksSpec, err := parseSpecs(arcadeJSON)
	if err != nil {
		return "", err
	}

	arcadePaths := getOrCreateMap(arcadeSpec, "paths")
	mergeChainstacksPaths(arcadePaths, chaintracksSpec, pathPrefix)
	mergeChaintracksTags(arcadeSpec, chaintracksSpec)
	mergeChaintracksSchemas(arcadeSpec, chaintracksSpec)
	updatePrefixedPathTags(arcadePaths, pathPrefix)

	mergedJSON, err := json.MarshalIndent(arcadeSpec, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal merged spec: %w", err)
	}

	return string(mergedJSON), nil
}

func parseSpecs(arcadeJSON string) (map[string]interface{}, map[string]interface{}, error) {
	var arcadeSpec map[string]interface{}
	if err := json.Unmarshal([]byte(arcadeJSON), &arcadeSpec); err != nil {
		return nil, nil, fmt.Errorf("failed to parse arcade spec: %w", err)
	}

	var chaintracksSpec map[string]interface{}
	if err := yaml.Unmarshal(chaintracksOpenAPIYAML, &chaintracksSpec); err != nil {
		return nil, nil, fmt.Errorf("failed to parse chaintracks spec: %w", err)
	}

	return arcadeSpec, chaintracksSpec, nil
}

func getOrCreateMap(parent map[string]interface{}, key string) map[string]interface{} {
	m, ok := parent[key].(map[string]interface{})
	if !ok {
		m = make(map[string]interface{})
		parent[key] = m
	}

	return m
}

func mergeChainstacksPaths(arcadePaths, chaintracksSpec map[string]interface{}, pathPrefix string) {
	chaintracksPaths, ok := chaintracksSpec["paths"].(map[string]interface{})
	if !ok {
		return
	}

	for path, pathItem := range chaintracksPaths {
		if isCDNOnlyPath(path) {
			continue
		}

		isLegacy := isLegacyPathItem(pathItem)
		arcadePaths[buildPrefixedPath(path, pathPrefix, isLegacy)] = pathItem
	}
}

func isLegacyPathItem(pathItem interface{}) bool {
	pathItemMap, ok := pathItem.(map[string]interface{})
	if !ok {
		return false
	}

	for _, operation := range pathItemMap {
		if isLegacyOperation(operation) {
			return true
		}
	}

	return false
}

func isLegacyOperation(operation interface{}) bool {
	opMap, ok := operation.(map[string]interface{})
	if !ok {
		return false
	}

	tags, ok := opMap["tags"].([]interface{})
	if !ok {
		return false
	}

	for _, tag := range tags {
		if tagStr, ok := tag.(string); ok && tagStr == "Legacy" {
			return true
		}
	}

	return false
}

func buildPrefixedPath(path, pathPrefix string, isLegacy bool) string {
	if isLegacy {
		return pathPrefix + "/v1" + path
	}

	return pathPrefix + path
}

func mergeChaintracksTags(arcadeSpec, chaintracksSpec map[string]interface{}) {
	chaintracksTags, hasTags := chaintracksSpec["tags"].([]interface{})
	if !hasTags {
		return
	}

	arcadeTags, _ := arcadeSpec["tags"].([]interface{})

	for _, tag := range chaintracksTags {
		tagMap, ok := tag.(map[string]interface{})
		if !ok {
			continue
		}

		if name, ok := tagMap["name"].(string); ok {
			tagMap["name"] = prefixChaintracksTag(name)
		}

		arcadeTags = append(arcadeTags, tagMap)
	}

	arcadeSpec["tags"] = arcadeTags
}

func prefixChaintracksTag(name string) string {
	if name == "Legacy" {
		return "chaintracks-v1"
	}

	return "chaintracks-" + name
}

func mergeChaintracksSchemas(arcadeSpec, chaintracksSpec map[string]interface{}) {
	chaintracksComponents, ok := chaintracksSpec["components"].(map[string]interface{})
	if !ok {
		return
	}

	chaintracksSchemas, ok := chaintracksComponents["schemas"].(map[string]interface{})
	if !ok {
		return
	}

	arcadeComponents := getOrCreateMap(arcadeSpec, "components")
	arcadeSchemas := getOrCreateMap(arcadeComponents, "schemas")

	for schemaName, schema := range chaintracksSchemas {
		arcadeSchemas["Chaintracks"+schemaName] = schema
	}
}

func updatePrefixedPathTags(arcadePaths map[string]interface{}, pathPrefix string) {
	for path, pathItem := range arcadePaths {
		if !strings.HasPrefix(path, pathPrefix) {
			continue
		}

		pathItemMap, ok := pathItem.(map[string]interface{})
		if !ok {
			continue
		}

		for method, operation := range pathItemMap {
			operationMap, ok := operation.(map[string]interface{})
			if !ok {
				continue
			}

			updateOperationTags(operationMap)
			updateSchemaRefs(operationMap, "Chaintracks")
			pathItemMap[method] = operationMap
		}
	}
}

func updateOperationTags(operationMap map[string]interface{}) {
	tags, ok := operationMap["tags"].([]interface{})
	if !ok {
		return
	}

	for i, tag := range tags {
		tagStr, ok := tag.(string)
		if !ok {
			continue
		}

		switch tagStr {
		case "v2", "CDN":
			tags[i] = "chaintracks-" + tagStr
		case "Legacy":
			tags[i] = "chaintracks-v1"
		}
	}
}

// isCDNOnlyPath returns true if the path is a CDN-only endpoint that should be excluded.
// We exclude the CDN health check since arcade has its own /health endpoint.
func isCDNOnlyPath(path string) bool {
	return path == "/health" // CDN health check - arcade has its own /health
}

// updateSchemaRefs recursively updates schema references by adding a prefix.
func updateSchemaRefs(obj interface{}, prefix string) {
	switch v := obj.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if key == "$ref" {
				updateSchemaRef(v, key, val, prefix)
			} else {
				updateSchemaRefs(val, prefix)
			}
		}
	case []interface{}:
		for _, item := range v {
			updateSchemaRefs(item, prefix)
		}
	}
}

func updateSchemaRef(m map[string]interface{}, key string, val interface{}, prefix string) {
	refStr, ok := val.(string)
	if !ok || !strings.HasPrefix(refStr, schemaRefPrefix) {
		return
	}

	m[key] = schemaRefPrefix + prefix + refStr[len(schemaRefPrefix):]
}
