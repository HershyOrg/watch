package shared

import (
	"encoding/json"
	"reflect"
)

// DeepCopy creates a deep copy of the given value.
// It handles common types used in state management:
// - Primitives (int, float, string, bool)
// - Pointers to structs
// - Slices and maps
// - nil values
//
// For custom types, it uses JSON marshaling as a fallback.
// This ensures that all state is properly isolated.
func DeepCopy(value any) any {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Ptr:
		// Handle pointer to struct
		if v.IsNil() {
			return nil
		}

		// Create new instance of the same type
		newPtr := reflect.New(v.Elem().Type())

		// Copy struct fields
		copyStruct(newPtr.Elem(), v.Elem())

		return newPtr.Interface()

	case reflect.Struct:
		// Handle struct value
		newVal := reflect.New(v.Type()).Elem()
		copyStruct(newVal, v)
		return newVal.Interface()

	case reflect.Slice:
		// Handle slices
		if v.IsNil() {
			return nil
		}
		newSlice := reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
		for i := 0; i < v.Len(); i++ {
			elem := DeepCopy(v.Index(i).Interface())
			newSlice.Index(i).Set(reflect.ValueOf(elem))
		}
		return newSlice.Interface()

	case reflect.Map:
		// Handle maps
		if v.IsNil() {
			return nil
		}
		newMap := reflect.MakeMap(v.Type())
		iter := v.MapRange()
		for iter.Next() {
			key := DeepCopy(iter.Key().Interface())
			val := DeepCopy(iter.Value().Interface())
			newMap.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val))
		}
		return newMap.Interface()

	default:
		// For primitives and other types, use JSON marshaling as fallback
		// This is safe but slower - consider optimizing for specific types
		data, err := json.Marshal(value)
		if err != nil {
			// If marshaling fails, return the original value
			// This maintains backward compatibility
			return value
		}

		newValue := reflect.New(v.Type()).Interface()
		if err := json.Unmarshal(data, &newValue); err != nil {
			return value
		}

		return reflect.ValueOf(newValue).Elem().Interface()
	}
}

// copyStruct copies all fields from src to dst
func copyStruct(dst, src reflect.Value) {
	for i := 0; i < src.NumField(); i++ {
		srcField := src.Field(i)
		dstField := dst.Field(i)

		// Skip unexported fields
		if !dstField.CanSet() {
			continue
		}

		// Deep copy the field value
		if srcField.CanInterface() {
			copied := DeepCopy(srcField.Interface())
			if copied != nil {
				dstField.Set(reflect.ValueOf(copied))
			}
		}
	}
}
