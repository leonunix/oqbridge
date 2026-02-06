package util

import "path"

// MatchWildcard reports whether name matches the wildcard pattern.
// The pattern syntax is the same as path.Match: '*' matches any sequence
// of non-separator characters, '?' matches any single character, and
// '[…]' matches character ranges.
func MatchWildcard(pattern, name string) bool {
	matched, _ := path.Match(pattern, name)
	return matched
}
