#!/bin/bash
# Posts a Markdown test-results summary to $GITHUB_STEP_SUMMARY.
# Args:
#   $1: space-separated list of Gradle module names (e.g. "ambry-account ambry-utils")
#   $2: a job-id-suffix used as the section heading (e.g. "clustermap")
# Reads JUnit XML reports from <module>/build/test-results/test/*.xml.
set -euo pipefail

MODULES="$1"
JOB_ID_SUFFIX="$2"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"

echo "## ${JOB_ID_SUFFIX} unit tests" >> "$SUMMARY"
echo "" >> "$SUMMARY"
echo "| Module | Tests | Passed | Failed | Skipped | Time |" >> "$SUMMARY"
echo "|--------|------:|-------:|-------:|--------:|-----:|" >> "$SUMMARY"

TOTAL_TESTS=0
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0

# Sum a numeric XML attribute across all test-result XML files in a directory.
# Args: $1 attribute name, $2 directory of XMLs
sum_attr() {
  grep -hoE "$1=\"[0-9.]+\"" "$2"/*.xml 2>/dev/null \
    | grep -oE '[0-9.]+' \
    | awk '{s+=$1} END {if (s == int(s)) print int(s); else printf "%.1f", s}'
}

for MOD in $MODULES; do
  XML_DIR="$MOD/build/test-results/test"
  if [ ! -d "$XML_DIR" ] || ! ls "$XML_DIR"/*.xml >/dev/null 2>&1; then
    echo "| \`$MOD\` | (no results) | - | - | - | - |" >> "$SUMMARY"
    continue
  fi

  TESTS=$(sum_attr 'tests' "$XML_DIR")
  FAILURES=$(sum_attr 'failures' "$XML_DIR")
  ERRORS=$(sum_attr 'errors' "$XML_DIR")
  SKIPPED=$(sum_attr 'skipped' "$XML_DIR")
  TIME=$(sum_attr 'time' "$XML_DIR")

  TESTS=${TESTS:-0}; FAILURES=${FAILURES:-0}; ERRORS=${ERRORS:-0}; SKIPPED=${SKIPPED:-0}; TIME=${TIME:-0}
  FAILED=$((FAILURES + ERRORS))
  PASSED=$((TESTS - FAILED - SKIPPED))

  if [ "$FAILED" -gt 0 ]; then MARK=":x:"; else MARK=":white_check_mark:"; fi
  echo "| $MARK \`$MOD\` | $TESTS | $PASSED | $FAILED | $SKIPPED | ${TIME}s |" >> "$SUMMARY"

  TOTAL_TESTS=$((TOTAL_TESTS + TESTS))
  TOTAL_PASSED=$((TOTAL_PASSED + PASSED))
  TOTAL_FAILED=$((TOTAL_FAILED + FAILED))
  TOTAL_SKIPPED=$((TOTAL_SKIPPED + SKIPPED))
done

echo "" >> "$SUMMARY"
echo "**Total:** $TOTAL_TESTS tests · $TOTAL_PASSED passed · $TOTAL_FAILED failed · $TOTAL_SKIPPED skipped" >> "$SUMMARY"

# If anything failed, list the failing testcases (suite.method) for one-click triage.
if [ "$TOTAL_FAILED" -gt 0 ]; then
  echo "" >> "$SUMMARY"
  echo "### Failing tests" >> "$SUMMARY"
  echo "" >> "$SUMMARY"
  for MOD in $MODULES; do
    XML_DIR="$MOD/build/test-results/test"
    [ -d "$XML_DIR" ] || continue
    grep -lE '<failure|<error' "$XML_DIR"/*.xml 2>/dev/null | while read -r F; do
      # Extract suite name (testsuite[name="..."]).
      SUITE=$(grep -m1 -oE 'testsuite[[:space:]]+name="[^"]+"' "$F" | sed -E 's/.*name="([^"]+)".*/\1/')
      # Extract testcase names that have a <failure> or <error> child.
      # Each testcase block is delimited by <testcase ... > ... </testcase> (or self-closing).
      # Regex requires a whitespace boundary before `name=` so we capture the
      # testcase's `name=` attribute, not `classname=` (which contains "name=").
      awk -v suite="$SUITE" '
        /<testcase/ {
          match($0, /[ \t]name="[^"]+"/);
          if (RSTART > 0) {
            name = substr($0, RSTART+7, RLENGTH-8);
            has_fail = 0;
          }
        }
        /<failure|<error/ { has_fail = 1 }
        /<\/testcase>/ {
          if (has_fail && name) print "- `" suite "." name "`";
          name = ""; has_fail = 0;
        }
      ' "$F" >> "$SUMMARY"
    done
  done
fi
