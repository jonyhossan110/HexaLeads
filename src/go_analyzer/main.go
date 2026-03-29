package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const defaultTimeout = 20 * time.Second
const maxBodyBytes = 1 << 20 // 1 MiB

var securityHeaders = []string{
	"Strict-Transport-Security",
	"X-Content-Type-Options",
	"X-Frame-Options",
	"Content-Security-Policy",
	"Referrer-Policy",
	"Permissions-Policy",
	"X-XSS-Protection",
}

var techPatterns = map[string]*regexp.Regexp{
	"WordPress": regexp.MustCompile(`(?i)wp-content|wp-json|wordpress`),
	"Drupal":    regexp.MustCompile(`(?i)drupal`),
	"Joomla":    regexp.MustCompile(`(?i)joomla`),
	"Shopify":   regexp.MustCompile(`(?i)shopify`),
	"React":     regexp.MustCompile(`(?i)react(?:\.js)?|react-dom`),
	"Next.js":   regexp.MustCompile(`(?i)next(?:\.js)?`),
	"Angular":   regexp.MustCompile(`(?i)angular(?:\.js)?`),
	"Vue.js":    regexp.MustCompile(`(?i)vue(?:\.js)?`),
}

type BusinessRecord struct {
	Name        string `json:"name"`
	Website     string `json:"website"`
	Phone       string `json:"phone"`
	Source      string `json:"source"`
	SearchQuery string `json:"searchQuery"`
	ScrapedAt   string `json:"scraped_at"`
}

type AnalyzedRecord struct {
	Name                   string            `json:"name"`
	Website                string            `json:"website"`
	Phone                  string            `json:"phone"`
	Source                 string            `json:"source"`
	SearchQuery            string            `json:"searchQuery"`
	ScrapedAt              string            `json:"scraped_at"`
	Reachable              bool              `json:"reachable"`
	StatusCode             int               `json:"status_code"`
	ResponseTimeMs         int64             `json:"response_time_ms"`
	TechStack              []string          `json:"tech_stack"`
	SecurityHeaders        map[string]string `json:"security_headers"`
	MissingSecurityHeaders []string          `json:"missing_security_headers"`
	Error                  string            `json:"error,omitempty"`
}

func main() {
	urlArg := flag.String("url", "", "Single website URL to analyze")
	inputFile := flag.String("input", "output/businesses.json", "JSON input file containing business records")
	outputFile := flag.String("output", "output/analyzed.json", "Optional output JSON file")
	flag.Parse()

	var analyzed []AnalyzedRecord
	var err error

	if *urlArg != "" {
		analyzed, err = analyzeSingleURL(*urlArg)
	} else {
		analyzed, err = analyzeInputFile(*inputFile)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "Analysis failed:", err)
		os.Exit(1)
	}

	payload, marshalErr := json.MarshalIndent(analyzed, "", "  ")
	if marshalErr != nil {
		fmt.Fprintln(os.Stderr, "Unable to encode JSON:", marshalErr)
		os.Exit(1)
	}

	if *outputFile != "" {
		if writeErr := os.WriteFile(*outputFile, payload, 0o644); writeErr != nil {
			fmt.Fprintln(os.Stderr, "Unable to write output file:", writeErr)
			os.Exit(1)
		}
		fmt.Println("Saved analysis to", *outputFile)
	} else {
		fmt.Println(string(payload))
	}
}

func analyzeInputFile(path string) ([]AnalyzedRecord, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var businesses []BusinessRecord
	if err := json.Unmarshal(content, &businesses); err != nil {
		return nil, err
	}

	if len(businesses) == 0 {
		return nil, errors.New("input file contains no businesses")
	}

	return analyzeBusinesses(businesses), nil
}

func analyzeSingleURL(rawURL string) ([]AnalyzedRecord, error) {
	record := BusinessRecord{Website: rawURL}
	return analyzeBusinesses([]BusinessRecord{record}), nil
}

func analyzeBusinesses(businesses []BusinessRecord) []AnalyzedRecord {
	results := make([]AnalyzedRecord, 0, len(businesses))
	for _, business := range businesses {
		analysis := AnalyzedRecord{
			Name:        business.Name,
			Website:     business.Website,
			Phone:       business.Phone,
			Source:      business.Source,
			SearchQuery: business.SearchQuery,
			ScrapedAt:   business.ScrapedAt,
		}

		if business.Website == "" {
			analysis.Error = "missing website"
			results = append(results, analysis)
			continue
		}

		cleanURL, err := ensureURL(business.Website)
		if err != nil {
			analysis.Error = fmt.Sprintf("invalid website URL: %v", err)
			results = append(results, analysis)
			continue
		}

		start := time.Now()
		headers, body, statusCode, fetchErr := fetchWebsite(cleanURL)
		analysis.ResponseTimeMs = time.Since(start).Milliseconds()
		analysis.StatusCode = statusCode
		if fetchErr != nil {
			analysis.Error = fetchErr.Error()
			results = append(results, analysis)
			continue
		}

		analysis.Reachable = statusCode >= 200 && statusCode < 400
		analysis.TechStack = detectTechStack(headers, body)
		analysis.SecurityHeaders, analysis.MissingSecurityHeaders = analyzeSecurityHeaders(headers)
		results = append(results, analysis)
	}
	return results
}

func ensureURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	if parsed.Host == "" {
		return "", errors.New("missing host")
	}
	return parsed.String(), nil
}

func fetchWebsite(targetURL string) (http.Header, string, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("User-Agent", "GeoLeadsX/1.0 (+https://example.com)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()

	limitedReader := io.LimitReader(resp.Body, maxBodyBytes)
	bodyBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		return resp.Header, "", resp.StatusCode, err
	}

	return resp.Header, string(bodyBytes), resp.StatusCode, nil
}

func detectTechStack(headers http.Header, body string) []string {
	found := make(map[string]struct{})
	if server := headers.Get("Server"); server != "" {
		found[strings.TrimSpace(server)] = struct{}{}
	}
	if poweredBy := headers.Get("X-Powered-By"); poweredBy != "" {
		found[strings.TrimSpace(poweredBy)] = struct{}{}
	}

	lowerBody := strings.ToLower(body)
	for name, pattern := range techPatterns {
		if pattern.MatchString(lowerBody) {
			found[name] = struct{}{}
		}
	}

	results := make([]string, 0, len(found))
	for tech := range found {
		results = append(results, tech)
	}
	return results
}

func analyzeSecurityHeaders(headers http.Header) (map[string]string, []string) {
	present := make(map[string]string)
	missing := make([]string, 0, len(securityHeaders))

	for _, header := range securityHeaders {
		value := strings.TrimSpace(headers.Get(header))
		if value != "" {
			present[header] = value
		} else {
			missing = append(missing, header)
		}
	}
	return present, missing
}
