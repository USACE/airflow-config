import logging
import re


class MaskSensitiveInfoFilter(logging.Filter):
    def filter(self, record):
        # Get the original message from the log record
        original_msg = record.getMessage()

        # Apply regex to mask the API key
        masked_msg = re.sub(r"key=[^\s&]+", "key=***", original_msg)

        # Update the message of the log record with the masked message
        record.msg = masked_msg

        # Return True to indicate that the log record should be processed further
        return True
