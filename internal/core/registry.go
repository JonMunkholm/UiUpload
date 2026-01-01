package core

import (
	"fmt"
	"sort"
	"sync"
)

var (
	registry   = make(map[string]TableDefinition)
	registryMu sync.RWMutex
)

// Register adds a table definition to the registry.
// Panics if a table with the same key is already registered.
func Register(def TableDefinition) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[def.Info.Key]; exists {
		panic(fmt.Sprintf("table already registered: %s", def.Info.Key))
	}

	// Populate Columns from FieldSpecs if not set
	if len(def.Info.Columns) == 0 && len(def.FieldSpecs) > 0 {
		def.Info.Columns = make([]string, len(def.FieldSpecs))
		for i, spec := range def.FieldSpecs {
			def.Info.Columns[i] = spec.Name
		}
	}

	registry[def.Info.Key] = def
}

// Get returns a table definition by key.
// Returns false if not found.
func Get(key string) (TableDefinition, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	def, ok := registry[key]
	return def, ok
}

// All returns all registered table definitions.
// Sorted by group then by key for consistent ordering.
func All() []TableDefinition {
	registryMu.RLock()
	defer registryMu.RUnlock()

	result := make([]TableDefinition, 0, len(registry))
	for _, def := range registry {
		result = append(result, def)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Info.Group != result[j].Info.Group {
			return result[i].Info.Group < result[j].Info.Group
		}
		return result[i].Info.Key < result[j].Info.Key
	})

	return result
}

// ByGroup returns all table definitions for a specific group.
// Sorted by key for consistent ordering.
func ByGroup(group string) []TableDefinition {
	registryMu.RLock()
	defer registryMu.RUnlock()

	var result []TableDefinition
	for _, def := range registry {
		if def.Info.Group == group {
			result = append(result, def)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Info.Key < result[j].Info.Key
	})

	return result
}

// Groups returns all unique group names.
// Sorted alphabetically.
func Groups() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	seen := make(map[string]bool)
	for _, def := range registry {
		seen[def.Info.Group] = true
	}

	groups := make([]string, 0, len(seen))
	for g := range seen {
		groups = append(groups, g)
	}

	sort.Strings(groups)
	return groups
}

// TableCount returns the number of registered tables.
func TableCount() int {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return len(registry)
}

// Clear removes all registered tables.
// Primarily useful for testing.
func Clear() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = make(map[string]TableDefinition)
}
