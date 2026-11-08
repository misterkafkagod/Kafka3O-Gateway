package httpapi

import "strings"

// neutralizeCSVCell stops spreadsheets from evaluating a cell as a formula.
func neutralizeCSVCell(value string) string {
	if value != "" && strings.ContainsRune("=+-@\t\r", rune(value[0])) {
		return "'" + value
	}
	return value
}
