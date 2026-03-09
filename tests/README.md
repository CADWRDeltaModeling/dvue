# dvue Tests

Test suite for the dvue package.

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dvue

# Run specific test file
pytest tests/test_dataui.py

# Run with verbose output
pytest -v
```

## Test Structure

- `test_dataui.py` - Tests for DataUIManager
- `test_actions.py` - Tests for action handlers
- `test_utils.py` - Tests for utility functions
- `test_tsdataui.py` - Tests for TimeSeriesDataUI

## Writing Tests

Tests should follow pytest conventions and include:
- Clear test names describing what is being tested
- Arrange-Act-Assert pattern
- Appropriate fixtures for test data
- Mock external dependencies when appropriate
