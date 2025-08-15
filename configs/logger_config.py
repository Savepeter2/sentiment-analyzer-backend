import os
import sys
from typing import Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class LogFileFormatError(ValueError):
    """Raised when the log file format is invalid"""

    pass


class InvalidInputTypeError(ValueError):
    """Raised when input arguments are not strings"""

    pass


def create_log_file(
    log_file_name: str, error_log_file_name: str,
    key_word_log_file_name: str,
      var_dir: str
) -> Tuple[str, str]:
    """
    Function to create a log file

    Args:
    log_file_name (str): Name of the log file
    error_log_file_name (str): Name of the error log file
    var_dir (str): Name of the directory to store the log files

    Returns:
    Tuple[str, str]: A tuple containing the path to the log file
    and the path to the error log file

    """

    if (
        isinstance(log_file_name, str)
        and isinstance(error_log_file_name, str)
        and isinstance(var_dir, str)
    ):
        if log_file_name.endswith(".log") and error_log_file_name.endswith(".log"):
            curr_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            var_dir = os.path.join(curr_dir, var_dir)

            if not os.path.exists(var_dir):
                os.makedirs(var_dir)
            log_dir = os.path.join(var_dir, "log")

            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_file_path = os.path.join(log_dir, log_file_name)
            if not os.path.exists(log_file_path):
                with open(log_file_path, "w") as f:
                    f.write("")

            error_log_file_path = os.path.join(log_dir, error_log_file_name)

            if not os.path.exists(error_log_file_path):
                with open(error_log_file_path, "w") as f:
                    f.write("")
            
            keyword_log_file_path = os.path.join(log_dir, "keyword.log")
            if not os.path.exists(keyword_log_file_path):
                with open(keyword_log_file_path, "w") as f:
                    f.write("")

            return log_file_path, error_log_file_path, keyword_log_file_path
        else:
            raise LogFileFormatError("Invalid file format. Only log files are allowed")

    else:
        raise InvalidInputTypeError(
            "Invalid input arguments. Input arguments must be strings"
        )



log_file_name = "info.log"
error_log_file_name = "error.log"
key_word_log_file_name = "keyword.log"
var_dir = "var"

log_file_path, error_log_file_path, keyword_log_file_path = create_log_file(
    log_file_name, error_log_file_name,
    key_word_log_file_name, var_dir 
)

# print("current directory:", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))