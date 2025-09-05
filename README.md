# congress-bonhomie

## Fetch Congressional Speeches

This project can retrieve speeches from the Congressional Record via the GovInfo API and save them locally for analysis.

### Prerequisites
- Set a GovInfo API key in your environment:
  - macOS/Linux: `export GOVINFO_API_KEY=YOUR_KEY`
  - Windows (PowerShell): `$Env:GOVINFO_API_KEY = "YOUR_KEY"`
- Python 3.8+ and `requests`:
  - `pip install requests`

### Option 1: Run the Script
- Script: `fetch_congressional_speeches.py:1`
- Example:
  - `python fetch_congressional_speeches.py --start 2024-09-01 --end 2024-09-05 --out data --csv`
- Outputs:
  - JSONL: `data/speeches_<start>_to_<end>.jsonl`
  - CSV: `data/speeches_<start>_to_<end>.csv` (when `--csv` is used)

Flags
- `--max-packages`: limit packages (for testing)
- `--max-granules`: limit granules per package (for testing)
- `--rate-delay`: delay between API calls (seconds)

### Option 2: Use the Notebook
- Notebook: `SpeechesOverTime.ipynb:1`
- Steps:
  - Open in VS Code (Jupyter extension) or Jupyter Lab/Notebook.
  - Ensure `GOVINFO_API_KEY` is set (or enter it when prompted in the first cell).
  - Adjust the date range in the “Driver” cell and run cells.
- Outputs:
  - JSONL: `data/speeches_<start>_to_<end>.jsonl`
  - Optional CSV: uncomment the `jsonl_to_csv` call in the final cell.

### Notes
- Start with smaller date ranges to validate, then scale up.
- The parser extracts `<speaking>` blocks from CREC XML and falls back to paragraphs when needed.
- Be mindful of API rate limits; defaults include modest delays and pagination.
