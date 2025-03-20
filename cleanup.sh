#!/bin/bash

# Find the repository root dynamically (assumes you're running from within the repo)
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)

# If not in a git repository, use the current directory as fallback
if [ -z "$REPO_ROOT" ]; then
    REPO_ROOT=$(pwd)
    echo "Not in a git repository, using current directory: $REPO_ROOT"
else
    echo "Repository root found at: $REPO_ROOT"
fi

# Target folder to remove
TARGET_FOLDER="spark/checkpoints"

# Full path to the target folder
FULL_PATH="$REPO_ROOT/$TARGET_FOLDER"

# Check if the folder exists
if [ -d "$FULL_PATH" ]; then
    echo "Removing folder: $FULL_PATH"
    rm -rf "$FULL_PATH"
    echo "Folder removed successfully."
else
    echo "Folder not found: $FULL_PATH"
fi

# Clean up docker volumes
echo "Cleaning up docker volumes..."
docker volume prune -a -f