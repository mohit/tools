# Testing Documentation

## Test Coverage

The Apple Health Export tool includes comprehensive test coverage with 48 unit tests covering all major functionality.

## Test Suite Overview

### Test Files

1. **`tests/test_health_export.py`** - Tests for `health_export.py`
2. **`tests/test_health_parser.py`** - Tests for `health_parser.py`
3. **`tests/fixtures/sample_export.xml`** - Sample Apple Health export data

### Running Tests

```bash
# Run all tests
python3 -m unittest discover -s tests -p "test_*.py" -v

# Run specific test file
python3 -m unittest tests.test_health_export -v
python3 -m unittest tests.test_health_parser -v

# Run specific test
python3 -m unittest tests.test_health_export.TestHealthExport.test_extract_export_success -v

# Using make
make test          # Run tests
make test-verbose  # Run with verbose output
make test-cov      # Run with coverage report
```

## Test Categories

### health_export.py Tests (18 tests)

#### Core Functionality Tests

1. **`test_trigger_health_export_success`** - Verify AppleScript trigger works
2. **`test_trigger_health_export_failure`** - Handle osascript failures
3. **`test_trigger_health_export_not_macos`** - Handle missing osascript (non-macOS)
4. **`test_find_health_export_found`** - Find export files in directory
5. **`test_find_health_export_not_found`** - Handle missing export files
6. **`test_find_health_export_multiple_files`** - Find most recent export
7. **`test_extract_export_success`** - Extract valid ZIP files
8. **`test_extract_export_file_not_found`** - Handle missing ZIP files
9. **`test_extract_export_invalid_zip`** - Handle corrupted ZIP files
10. **`test_extract_export_custom_output_dir`** - Custom extraction directory
11. **`test_get_export_info_success`** - Get info from extracted export
12. **`test_get_export_info_directory_not_found`** - Handle missing directories
13. **`test_get_export_info_no_xml`** - Handle missing export.xml
14. **`test_get_export_info_alternate_location`** - Find export.xml in alternate locations

#### CLI Tests

15. **`test_cli_export_command`** - Test export CLI command
16. **`test_cli_find_command_not_found`** - Test find command with no results
17. **`test_cli_extract_command_success`** - Test extract CLI command
18. **`test_cli_info_command_missing_dir`** - Test info command validation

### health_parser.py Tests (30 tests)

#### Parser Core Tests

1. **`test_parser_initialization`** - Verify parser initializes correctly
2. **`test_parse_success`** - Parse valid XML successfully
3. **`test_parse_invalid_xml`** - Handle malformed XML
4. **`test_parse_missing_file`** - Handle missing XML files
5. **`test_get_record_types`** - Extract unique record types
6. **`test_get_record_types_no_parse`** - Handle unparsed state
7. **`test_get_workout_types`** - Extract unique workout types
8. **`test_get_workout_types_no_parse`** - Handle unparsed state

#### Record Export Tests

9. **`test_export_records_to_csv_all_records`** - Export all records
10. **`test_export_records_to_csv_filtered_by_type`** - Filter by record type
11. **`test_export_records_to_csv_filtered_by_date`** - Filter by date range
12. **`test_export_records_to_csv_with_metadata`** - Include metadata in export
13. **`test_export_records_to_csv_no_matches`** - Handle no matching records
14. **`test_export_records_to_csv_not_parsed`** - Require parsing before export

#### Workout Export Tests

15. **`test_export_workouts_to_csv_all_workouts`** - Export all workouts
16. **`test_export_workouts_to_csv_filtered_by_type`** - Filter by workout type
17. **`test_export_workouts_to_csv_includes_statistics`** - Include workout stats
18. **`test_export_workouts_to_csv_includes_metadata`** - Include metadata
19. **`test_export_workouts_to_csv_no_matches`** - Handle no matching workouts
20. **`test_export_workouts_to_csv_not_parsed`** - Require parsing before export

#### Summary Stats Tests

21. **`test_get_summary_stats`** - Generate summary statistics
22. **`test_get_summary_stats_not_parsed`** - Handle unparsed state

#### CLI Tests

23. **`test_cli_list_types_command`** - Test list-types command
24. **`test_cli_list_workouts_command`** - Test list-workouts command
25. **`test_cli_summary_command`** - Test summary command
26. **`test_cli_export_records_command`** - Test export-records command
27. **`test_cli_export_records_missing_output`** - Validate required arguments
28. **`test_cli_export_workouts_command`** - Test export-workouts command
29. **`test_cli_with_date_filters`** - Test date range filtering
30. **`test_cli_invalid_xml_file`** - Handle invalid file paths

## Test Fixtures

### Sample Export Data

The test suite uses `tests/fixtures/sample_export.xml` which contains:

- **8 health records** across 5 different types:
  - Step count (3 records)
  - Heart rate (2 records)
  - Distance walking/running (1 record)
  - Active energy burned (1 record)
  - Sleep analysis (1 record)

- **3 workouts**:
  - Running (with heart rate statistics)
  - Walking
  - Cycling (with heart rate statistics)

- **2 activity summaries**

- **Metadata entries** for testing metadata extraction

## Test Results

```
Ran 48 tests in 0.068s

OK
```

All tests pass successfully on both macOS and Linux environments.

## Code Coverage

Key areas with test coverage:

- ✅ File operations (finding, extracting, reading)
- ✅ XML parsing and error handling
- ✅ CSV export with various filters
- ✅ Date range filtering (with timezone handling)
- ✅ Metadata extraction
- ✅ Workout statistics extraction
- ✅ CLI argument parsing and validation
- ✅ Error conditions and edge cases

## Known Limitations

1. **Platform-specific functionality**: The AppleScript trigger for opening Health.app only works on macOS. Tests mock this functionality on other platforms.

2. **Real Health.app integration**: Tests don't interact with the actual Health.app - they use mock data and mocked system calls.

3. **Large file performance**: Tests use small sample files. Real exports can be several GB and may have different performance characteristics.

## Adding New Tests

To add new tests:

1. Add test methods to the appropriate test class in `tests/test_health_export.py` or `tests/test_health_parser.py`
2. Use the existing fixtures or create new ones in `tests/fixtures/`
3. Follow the naming convention: `test_<functionality>_<scenario>`
4. Run the full test suite to ensure no regressions

Example:

```python
def test_export_records_new_feature(self):
    """Test description here."""
    parser = health_parser.HealthDataParser(self.sample_xml)
    parser.parse()

    # Test your functionality
    result = parser.some_new_method()

    # Assertions
    self.assertTrue(result)
```

## Continuous Testing

For development, you can use:

```bash
# Watch for changes and re-run tests (requires pytest-watch)
ptw

# Run tests on file change (using inotifywait on Linux)
while inotifywait -e modify health_*.py tests/*.py; do
    make test
done
```

## Bug Fixes Verified by Tests

1. **Timezone comparison bug** - Fixed in `health_parser.py:87-100`
   - Issue: Comparing offset-naive and offset-aware datetimes
   - Test: `test_export_records_to_csv_filtered_by_date`
   - Fix: Convert filter dates to timezone-aware datetimes

## Future Test Improvements

Potential areas for additional testing:

- [ ] Performance tests with large XML files
- [ ] Integration tests with real Health.app exports
- [ ] Memory usage tests for large datasets
- [ ] Concurrent access tests
- [ ] More edge cases for malformed data
- [ ] GPX file parsing (workout routes)
- [ ] Clinical Document (CDA) parsing
