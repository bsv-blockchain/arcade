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
func mergeOpenAPISpecs(arcadeJSON string, pathPrefix string) (string, error) {
	// Parse arcade spec (JSON from swagger)
	var arcadeSpec map[string]interface{}
	if err := json.Unmarshal([]byte(arcadeJSON), &arcadeSpec); err != nil {
		return "", fmt.Errorf("failed to parse arcade spec: %w", err)
	}

	// Parse chaintracks spec (YAML)
	var chaintracksSpec map[string]interface{}
	if err := yaml.Unmarshal(chaintracksOpenAPIYAML, &chaintracksSpec); err != nil {
		return "", fmt.Errorf("failed to parse chaintracks spec: %w", err)
	}

	// Get or create paths map in arcade spec
	arcadePaths, ok := arcadeSpec["paths"].(map[string]interface{})
	if !ok {
		arcadePaths = make(map[string]interface{})
		arcadeSpec["paths"] = arcadePaths
	}

	// Merge chaintracks paths with prefix
	if chaintracksPaths, ok := chaintracksSpec["paths"].(map[string]interface{}); ok {
		for path, pathItem := range chaintracksPaths {
			// Skip CDN-only health endpoint (arcade has its own /health)
			if isCDNOnlyPath(path) {
				continue
			}

			// Determine if this is a v1 legacy path by checking its tags
			isLegacyPath := false
			if pathItemMap, ok := pathItem.(map[string]interface{}); ok {
				for _, operation := range pathItemMap {
					if opMap, ok := operation.(map[string]interface{}); ok {
						if tags, ok := opMap["tags"].([]interface{}); ok {
							for _, tag := range tags {
								if tagStr, ok := tag.(string); ok && tagStr == "Legacy" {
									isLegacyPath = true
									break
								}
							}
						}
					}
					if isLegacyPath {
						break
					}
				}
			}

			// Add appropriate prefix based on path type
			var prefixedPath string
			if isLegacyPath {
				// v1 legacy paths need /v1 added
				prefixedPath = pathPrefix + "/v1" + path
			} else if strings.HasPrefix(path, "/v2/") {
				// v2 paths already have /v2 prefix
				prefixedPath = pathPrefix + path
			} else {
				// Other paths (shouldn't happen after CDN filtering)
				prefixedPath = pathPrefix + path
			}

			arcadePaths[prefixedPath] = pathItem
		}
	}

	// Merge tags
	arcadeTags, _ := arcadeSpec["tags"].([]interface{})
	if chaintracksTags, ok := chaintracksSpec["tags"].([]interface{}); ok {
		// Add chaintracks tags with a prefix to avoid confusion
		for _, tag := range chaintracksTags {
			if tagMap, ok := tag.(map[string]interface{}); ok {
				// Prefix tag name with "chaintracks-"
				if name, ok := tagMap["name"].(string); ok {
					// Rename "Legacy" to "v1" for consistency
					if name == "Legacy" {
						tagMap["name"] = "chaintracks-v1"
					} else {
						tagMap["name"] = "chaintracks-" + name
					}
				}
				arcadeTags = append(arcadeTags, tagMap)
			}
		}
		arcadeSpec["tags"] = arcadeTags
	}

	// Merge components/schemas if they exist
	arcadeComponents, ok := arcadeSpec["components"].(map[string]interface{})
	if !ok {
		arcadeComponents = make(map[string]interface{})
		arcadeSpec["components"] = arcadeComponents
	}

	if chaintracksComponents, ok := chaintracksSpec["components"].(map[string]interface{}); ok {
		if chaintracksSchemas, ok := chaintracksComponents["schemas"].(map[string]interface{}); ok {
			arcadeSchemas, ok := arcadeComponents["schemas"].(map[string]interface{})
			if !ok {
				arcadeSchemas = make(map[string]interface{})
				arcadeComponents["schemas"] = arcadeSchemas
			}

			// Prefix chaintracks schema names to avoid conflicts
			for schemaName, schema := range chaintracksSchemas {
				arcadeSchemas["Chaintracks"+schemaName] = schema
			}
		}
	}

	// Update all chaintracks path references to use prefixed tags
	for path, pathItem := range arcadePaths {
		if pathItemMap, ok := pathItem.(map[string]interface{}); ok {
			for method, operation := range pathItemMap {
				if operationMap, ok := operation.(map[string]interface{}); ok {
					if tags, ok := operationMap["tags"].([]interface{}); ok {
						for i, tag := range tags {
							if tagStr, ok := tag.(string); ok {
								// If it's a chaintracks tag, prefix it (rename Legacy to v1)
								if tagStr == "v2" || tagStr == "CDN" {
									tags[i] = "chaintracks-" + tagStr
								} else if tagStr == "Legacy" {
									tags[i] = "chaintracks-v1"
								}
							}
						}
					}

					// Update schema references for chaintracks endpoints
					if pathPrefix != "" && len(path) >= len(pathPrefix) && path[:len(pathPrefix)] == pathPrefix {
						updateSchemaRefs(operationMap, "Chaintracks")
					}

					pathItemMap[method] = operationMap
				}
			}
		}
	}

	// Marshal back to JSON
	mergedJSON, err := json.MarshalIndent(arcadeSpec, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal merged spec: %w", err)
	}

	return string(mergedJSON), nil
}

// isCDNOnlyPath returns true if the path is a CDN-only endpoint that should be excluded.
// We exclude the CDN health check since arcade has its own /health endpoint.
func isCDNOnlyPath(path string) bool {
	// Exclude only the CDN-specific health check
	// (We DO serve the CDN bootstrap files via static file serving)
	return path == "/health" // CDN health check - arcade has its own /health
}

// updateSchemaRefs recursively updates schema references by adding a prefix
func updateSchemaRefs(obj interface{}, prefix string) {
	switch v := obj.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if key == "$ref" {
				if refStr, ok := val.(string); ok {
					// Update component schema references
					if len(refStr) > 21 && refStr[:21] == "#/components/schemas/" {
						schemaName := refStr[21:]
						v[key] = "#/components/schemas/" + prefix + schemaName
					}
				}
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
