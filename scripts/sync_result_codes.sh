#!/bin/bash

# Filename of the Java class to be converted
JAVA_FILE="../java_pipeline/common/src/main/java/cz/vut/fit/domainradar/models/ResultCodes.java"
# Output filename for the Python file
PYTHON_FILE="../python_pipeline/common/result_codes.py"

# Start with an empty Python file
true > "$PYTHON_FILE"

# Flag to indicate if we are inside a block comment
inside_comment=0
current_comment=""

# Read the Java file line by line
while IFS= read -r line; do
    if [[ $line =~ ^[[:space:]]*/\*\* ]]; then
        # Starting a block comment
        inside_comment=1
    elif [[ $inside_comment -eq 1 && $line =~ \*/ ]]; then
        # Ending a block comment
        inside_comment=0
    elif [[ $inside_comment -eq 1 ]]; then
        # We are inside a comment block
        # Strip leading spaces and asterisk, then prepend with '#'
        current_comment+=$(echo "$line" | sed 's/^[[:space:]]*\**[[:space:]]*//')
        current_comment+=$'\n'
    elif [[ $line =~ public\ static\ final\ int ]]; then
        # Convert the constant definition to Python syntax
        echo "$line" | sed -E 's/ *public\ static\ final\ int\ ([A-Z_]+)\ =\ ([0-9]+);/\1 = \2/' >> "$PYTHON_FILE"
        echo "\"\"\"${current_comment::-1}\"\"\"" >> "$PYTHON_FILE"
        current_comment=""
    fi
done < "$JAVA_FILE"
