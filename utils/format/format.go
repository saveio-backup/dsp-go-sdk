package format

// BoolToStr. format bool value to string, if true, return "success", else return "fail"
func BoolToStr(value bool) string {
	if value {
		return "success"
	} else {
		return "fail"
	}
}

// StringsToMap. make string slices to struct
func StringsToMap(strs []string) map[string]struct{} {
	m := make(map[string]struct{})
	if len(strs) == 0 {
		return m
	}
	for _, val := range strs {
		m[val] = struct{}{}
	}
	return m
}

// RemoveDuplicated. remove duplicated slices
func RemoveDuplicated(input []string) []string {
	exist := make(map[string]struct{}, 0)
	output := make([]string, 0, len(input))
	for _, url := range input {
		if _, ok := exist[url]; ok {
			continue
		}
		exist[url] = struct{}{}
		output = append(output, url)
	}
	return output
}

// StringSliceToKeyMap. convert a string slice to map with default value ""
func StringSliceToKeyMap(input []string) map[string]string {
	m := make(map[string]string, len(input))
	for _, key := range input {
		m[key] = ""
	}
	return m
}
