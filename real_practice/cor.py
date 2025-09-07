import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, Iterator

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LogCorrelator:
    """
    Correlates request and response logs in a memory-efficient, streaming manner.
    """
    def __init__(self):
        self.pending_requests: Dict[str, Dict[str, Any]] = {}

    def _process_request_line(self, line: str):
        # This function is the same as your original
        try:
            timestamp_str, req_id, user_id = line.strip().split(',')
            if req_id in self.pending_requests and 'end_time' in self.pending_requests[req_id]:
                self.pending_requests[req_id].update({
                    'start_time': datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')),
                    'user_id': user_id
                })
            else:
                self.pending_requests[req_id] = {
                    'start_time': datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')),
                    'user_id': user_id
                }
        except (ValueError, IndexError):
            logging.warning(f"Skipping malformed request line: {line.strip()}")

    def _process_response_line(self, line: str) -> Iterator[Dict[str, Any]]:
        # This function is also the same as your original
        try:
            timestamp_str, resp_id, status_code = line.strip().split(',')
            end_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

            if resp_id in self.pending_requests and 'start_time' in self.pending_requests[resp_id]:
                req_info = self.pending_requests.pop(resp_id)
                duration = (end_time - req_info['start_time']).total_seconds() * 1000
                yield {
                    'request_id': resp_id,
                    'user_id': req_info['user_id'],
                    'status_code': int(status_code),
                    'duration_ms': int(duration)
                }
            else:
                self.pending_requests[resp_id] = {
                    'end_time': end_time,
                    'status_code': int(status_code)
                }
        except (ValueError, IndexError):
            logging.warning(f"Skipping malformed response line: {line.strip()}")

    # --- THIS IS THE CORRECTED METHOD ---
    def correlate(self, requests_file: str, responses_file: str) -> Iterator[Dict[str, Any]]:
        """
        Processes both log files by streaming through them one by one, which correctly
        handles files of different lengths and unordered entries.
        """
        logging.info(f"Processing requests from {requests_file}...")
        with open(requests_file, 'r') as f:
            for line in f:
                self._process_request_line(line)
        
        logging.info(f"Processing responses from {responses_file} and yielding matches...")
        with open(responses_file, 'r') as f:
            for line in f:
                yield from self._process_response_line(line)

        # After files are processed, log any remaining orphaned entries
        orphaned_count = len(self.pending_requests)
        if orphaned_count > 0:
            logging.warning(f"Found {orphaned_count} orphaned log entries that were not matched.")
        logging.info("Correlation complete.")


def main():
    parser = argparse.ArgumentParser(description="Correlate web service log files.")
    parser.add_argument("requests_file", help="Path to the requests log file.")
    parser.add_argument("responses_file", help="Path to the responses log file.")
    parser.add_argument("output_file", help="Path for the JSON output file.")
    args = parser.parse_args()

    correlator = LogCorrelator()
    with open(args.output_file, 'w') as f:
        # The list() consumes the generator and collects all results.
        results = list(correlator.correlate(args.requests_file, args.responses_file))
        json.dump(results, f, indent=2)
        
    logging.info(f"Successfully wrote {len(results)} correlated records to {args.output_file}")

# This makes the script runnable from the command line
if __name__ == "__main__":
    main()