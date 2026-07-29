# File Organizer

A Python script that organizes files from a chosen source folder by sorting them into category-based folders or moving them into common user folders such as Documents, Pictures, and Music.

## Requirements
- Python 3
- The script files should be in the same project folder

## Run the Script
Open a terminal in the project folder and run:

```bash
python3 file_organizer.py
```

If your system uses `python` instead of `python3`, you can run:

```bash
python file_organizer.py
```

## How to Use
1. Enter the full path to the folder you want to organize. Example: `/Users/<username>/Downloads/`
2. Choose one of the organization options:
   - 1: Review Organized Files — creates a dated folder inside the source directory and sorts files into subfolders by file type
   - 2: Save Files Directly to User Folders — moves files into standard folders such as Documents, Pictures, Music, and Movies
3. Review the output in the terminal. A log file is also created in the source folder.

## Output
- Console messages show the files that were moved and any issues encountered.
- A log file named `file_organizer_logs_<date>.txt` is created in the folder you organized.