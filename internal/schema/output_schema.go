package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type schemaTarget struct {
	Name string
	Type reflect.Type
}

type Bundle struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Schemas     map[string]Schema `json:"schemas"`
}

type Schema map[string]any

func WriteOutputSchemas(outDir string, pretty bool) ([]string, error) {
	targets := []schemaTarget{
		{Name: "chat_insights", Type: reflect.TypeOf(domain.ChatInsights{})},
		{Name: "participant_snapshot", Type: reflect.TypeOf(domain.ParticipantSnapshot{})},
		{Name: "message_record", Type: reflect.TypeOf(domain.MessageRecord{})},
		{Name: "user_profile", Type: reflect.TypeOf(domain.UserProfile{})},
	}

	if outDir == "" {
		outDir = filepath.Join(".", "data", "exports", "schemas")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create schema directory: %w", err)
	}

	paths := make([]string, 0, len(targets)+1)
	bundle := Bundle{
		GeneratedAt: time.Now().UTC(),
		Schemas:     make(map[string]Schema, len(targets)),
	}

	for _, target := range targets {
		s := buildRootSchema(target.Name, target.Type)
		path := filepath.Join(outDir, target.Name+".schema.json")
		if err := writeJSON(path, s, pretty); err != nil {
			return nil, err
		}
		bundle.Schemas[target.Name] = s
		paths = append(paths, path)
	}

	bundlePath := filepath.Join(outDir, "output_format.schema.bundle.json")
	if err := writeJSON(bundlePath, bundle, pretty); err != nil {
		return nil, err
	}
	paths = append(paths, bundlePath)
	sort.Strings(paths)
	return paths, nil
}

func buildRootSchema(name string, t reflect.Type) Schema {
	g := &generator{}
	schema := g.schemaForType(t, map[reflect.Type]bool{})
	title := titleFromSnake(name)
	schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	schema["title"] = title
	return schema
}

func titleFromSnake(name string) string {
	parts := strings.Fields(strings.ReplaceAll(strings.TrimSpace(name), "_", " "))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}

type generator struct{}

func (g *generator) schemaForType(t reflect.Type, seen map[reflect.Type]bool) Schema {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t == reflect.TypeOf(time.Time{}) {
		return Schema{"type": "string", "format": "date-time"}
	}
	if t.PkgPath() == "encoding/json" && t.Name() == "RawMessage" {
		return Schema{}
	}

	if seen[t] {
		return Schema{"type": "object"}
	}
	seen[t] = true
	defer delete(seen, t)

	switch t.Kind() {
	case reflect.Struct:
		properties := make(map[string]any)
		required := make([]string, 0)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}
			name, omitempty, include := jsonName(field)
			if !include {
				continue
			}
			properties[name] = g.schemaForType(field.Type, seen)
			if !omitempty {
				required = append(required, name)
			}
		}
		sort.Strings(required)
		result := Schema{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		if len(required) > 0 {
			result["required"] = required
		}
		return result
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return Schema{"type": "string"}
		}
		return Schema{"type": "array", "items": g.schemaForType(t.Elem(), seen)}
	case reflect.Array:
		return Schema{
			"type":     "array",
			"items":    g.schemaForType(t.Elem(), seen),
			"minItems": t.Len(),
			"maxItems": t.Len(),
		}
	case reflect.Map:
		return Schema{
			"type":                 "object",
			"additionalProperties": g.schemaForType(t.Elem(), seen),
		}
	case reflect.Interface:
		return Schema{}
	case reflect.String:
		return Schema{"type": "string"}
	case reflect.Bool:
		return Schema{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Schema{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return Schema{"type": "number"}
	default:
		return Schema{}
	}
}

func jsonName(field reflect.StructField) (name string, omitempty bool, include bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, false
	}
	if tag == "" {
		return field.Name, false, true
	}
	parts := strings.Split(tag, ",")
	name = strings.TrimSpace(parts[0])
	if name == "" {
		name = field.Name
	}
	for _, p := range parts[1:] {
		if strings.TrimSpace(p) == "omitempty" {
			omitempty = true
			break
		}
	}
	return name, omitempty, true
}

func writeJSON(path string, payload any, pretty bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}

	data, err := func() ([]byte, error) {
		if pretty {
			return json.MarshalIndent(payload, "", "  ")
		}
		return json.Marshal(payload)
	}()
	if err != nil {
		return fmt.Errorf("marshal json for %s: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write schema %s: %w", path, err)
	}
	return nil
}
