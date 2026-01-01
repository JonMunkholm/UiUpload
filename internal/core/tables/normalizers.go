package tables

import "strings"

// UsStates maps US state full names to their abbreviations.
var UsStates = map[string]string{
	"alabama":        "AL",
	"alaska":         "AK",
	"arizona":        "AZ",
	"arkansas":       "AR",
	"california":     "CA",
	"colorado":       "CO",
	"connecticut":    "CT",
	"delaware":       "DE",
	"florida":        "FL",
	"georgia":        "GA",
	"hawaii":         "HI",
	"idaho":          "ID",
	"illinois":       "IL",
	"indiana":        "IN",
	"iowa":           "IA",
	"kansas":         "KS",
	"kentucky":       "KY",
	"louisiana":      "LA",
	"maine":          "ME",
	"maryland":       "MD",
	"massachusetts":  "MA",
	"michigan":       "MI",
	"minnesota":      "MN",
	"mississippi":    "MS",
	"missouri":       "MO",
	"montana":        "MT",
	"nebraska":       "NE",
	"nevada":         "NV",
	"new hampshire":  "NH",
	"new jersey":     "NJ",
	"new mexico":     "NM",
	"new york":       "NY",
	"north carolina": "NC",
	"north dakota":   "ND",
	"ohio":           "OH",
	"oklahoma":       "OK",
	"oregon":         "OR",
	"pennsylvania":   "PA",
	"rhode island":   "RI",
	"south carolina": "SC",
	"south dakota":   "SD",
	"tennessee":      "TN",
	"texas":          "TX",
	"utah":           "UT",
	"vermont":        "VT",
	"virginia":       "VA",
	"washington":     "WA",
	"west virginia":  "WV",
	"wisconsin":      "WI",
	"wyoming":        "WY",
}

// NormalizeUsState converts US state names to their 2-letter abbreviations.
// If the input is already an abbreviation or not recognized, returns as-is.
func NormalizeUsState(s string) string {
	s = strings.TrimSpace(s)
	sLower := strings.ToLower(s)

	// Check if it's a full name
	if code, ok := UsStates[sLower]; ok {
		return code
	}

	// Check if already a valid 2-letter code
	sUpper := strings.ToUpper(s)
	for _, code := range UsStates {
		if sUpper == code {
			return code
		}
	}

	// Fallback: return original
	return s
}
