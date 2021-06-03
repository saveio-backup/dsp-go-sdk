package validator

import "regexp"

func ValidateDomainName(domain string) bool {
	if len(domain) == 0 {
		return false
	}
	stringRegex := regexp.MustCompile(`^[A-Za-z0-9]+$`)
	if stringRegex.MatchString(domain) {
		return true
	}
	// Golang does not support Perl syntax ((?
	// will throw out :
	// error parsing regexp: invalid or unsupported Perl syntax: `(?!`
	//patternStr := "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$"
	// use regular expression without Perl syntax
	regExp := regexp.MustCompile(`^(([a-zA-Z]{1})|([a-zA-Z]{1}[a-zA-Z]{1})|([a-zA-Z]{1}[0-9]{1})|([0-9]{1}[a-zA-Z]{1})|([a-zA-Z0-9][a-zA-Z0-9-_]{1,61}[a-zA-Z0-9]))\.([a-zA-Z]{2,6}|[a-zA-Z0-9-]{2,30}\.[a-zA-Z
]{2,3})$`)
	return regExp.MatchString(domain)
}
