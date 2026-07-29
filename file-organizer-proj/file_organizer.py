#!/usr/bin/env python3
"""
Process flow file-organizer:
1. Input full path of Folder to Organize.
2. Find file types to organize. 
3. Move to designated folders.
3. Generate a log file of the summary of how files are organized
"""

from pathlib import Path
from datetime import date
import shutil

def get_file_path():
    home_dir = Path.home()
    while True:
        full_path = Path(input('\nEnter Full Folder Path to Organize: (e.g. /Users/<username>/Downloads/) ').strip())
        if full_path.exists():
            while True:
                confirm = input('Choose an option: \n(1) Review Organized files? or \n(2) Save Files Directly to User Folders? \nChoice: ')
                if confirm == '1': return full_path, True
                elif confirm == '2': return full_path, False
                else: print('Incorrect input.')
        else: 
            print(f'Path {full_path} does not exists.')

""" 
Name: organize_files_for_review()
Description: Move files to segregated folders for user to review.
Limitations: Only works for individual files not for folders.
"""
def organize_files_for_review(src_directory):
    print(f'\nOrganizing files in {src_directory}...\n')

    # Get list of files from source directory
    files_list = [item for item in src_directory.iterdir() if item.is_file()]

    # Define extensions and their corresponding Folder destination for each file type
    text_types = [ {'foldername': 'Excel Files', 'extension': ['.csv', '.xlsx'] },
                    {'foldername': 'PDF Files', 'extension': ['.pdf'] },
                    {'foldername': 'Document Files', 'extension': ['.docx'] },
                    {'foldername': 'Plain Text Files', 'extension': ['.txt'] },
                    {'foldername': 'Powerpoint Files', 'extension': ['.ppt', '.pptx'] },
                    {'foldername': 'HTML Files', 'extension': ['.html'] },
                ]
    
    image_types = [ {'foldername': 'JPEG Files', 'extension': ['.jpg', '.jpeg'] },
                   {'foldername': 'PNG Files', 'extension': ['.png'] },
                   {'foldername': 'GIF Files', 'extension': ['.gif'] },
                   {'foldername': 'HEIC Files', 'extension': ['.heic'] },
                ]

    video_types = [ {'foldername': 'MOV files', 'extension': ['.mov'] },
                   {'foldername': 'MP4 files', 'extension': ['.mp4'] },
                ]
    
    audio_types = [ {'foldername': 'MP3 files', 'extension': ['.mp3', '.wav']} ]
    installer_types = [ {'foldername': 'DMG files', 'extension': ['.dmg']} ]

    filetypes_list = [text_types, image_types, video_types, audio_types, installer_types ]

    today = date.today().strftime('%m%d%Y')
    
    # Create a folder for files to be processed for the day. (Date when process is run)
    # Folder name = Files Organized - 01011990
    # Make folder if doesn't exist
    main_foldername = Path(str(src_directory) + '/' + 'Files Organized - '+ today)
    if not main_foldername.exists():
        main_foldername.mkdir()

    try:
        countmoved = 0
        notmoved = 0
        success_logs = []
        issue_logs = []
        summary_logs = []

        logs = [success_logs, issue_logs, summary_logs]
        # Process each file and validate its extension
        # If file exstension is found, move file to its designated folder
        for items in files_list:
            file_moved = False

            for types in filetypes_list:
                for ext in types:
                    extlist = ext.get('extension')
                    if items.suffix.lower() in extlist:
                        folder = ext.get('foldername')
                        destination = Path(str(main_foldername)+'/'+folder)
                        if not destination.exists():
                            destination.mkdir()
                            print(f'{destination} created')

                        check_file = Path(str(destination)+'/'+str(items.name))
                        if destination.is_dir():
                            if not check_file.is_file():
                                shutil.move(src=items, dst=destination)
                                success_logs.append(f'Moving this file: {items.name} in {destination}.')
                                file_moved = True
                                countmoved += 1
                            else: issue_logs.append(f'File already exists in {destination}.')
                        else: issue_logs.append(f'Error moving {items.name}. {destination} is not a valid directory.')
                        break
                if file_moved: break

            if not file_moved :
                issue_logs.append(f'This file is not moved: {items.name}.')
                notmoved += 1

        summary_logs.append(f'Total of files to process: {len(files_list)}.')
        summary_logs.append(f'Total of files moved: {countmoved}.')
        summary_logs.append(f'Total of files not moved: {notmoved}.')

    except Exception as e:
        print(e)
        issue_logs.append(e)
    
    create_log(logs, src_directory)

""" 
Name: organize_files()
Description: Move files to designated user main folders (i.e Downloads, Documents, Pictures etc.)
Limitations: Only works for individual files not for folders.
"""
def organize_files(src_directory):
    print(f'\nOrganizing files in {src_directory}...\n')

    home_path = Path.home()
    # Get list of files from source directory
    files_list = [item for item in src_directory.iterdir() if item.is_file()]

    # Define extensions and their corresponding Folder destination for each file type
    text_types = [ {'foldername': 'Documents', 
                    'extension': ['.csv', '.xlsx', '.pdf', '.docx', '.txt', '.ppt', '.pptx', '.html'] }
                ]
    
    image_types = [ {'foldername': 'Pictures', 
                    'extension': ['.jpg', '.jpeg', '.png', '.gif', '.heic'] }
                ]

    video_types = [ {'foldername': 'Movies',
                      'extension': ['.mov', '.mp4'] }
                ]
    
    audio_types = [ {'foldername': 'Music', 'extension': ['.mp3']} ]    

    filetypes_list = [text_types, image_types, video_types, audio_types,  ]
    
    try:
        countmoved = 0
        notmoved = 0
        success_logs = ['\nThe Following Files Successfully moved:\n']
        issue_logs = ['\nIssues encountered for the following:\n']
        summary_logs = ['\nSUMMARY:\n']

        logs = [success_logs, issue_logs, summary_logs]
        # Process each file and validate its extension
        # If file exstension is found, move file to its designated folder
        for items in files_list:
            file_moved = False
            for types in filetypes_list:
                for ext in types:
                    extlist = ext.get('extension')
                    if items.suffix.lower() in extlist:
                        folder = ext.get('foldername')
                        destination = Path(str(home_path)+'/'+folder)
                        if not destination.exists():
                            issue_logs.append(f'{destination} does not exist. {items.name} not moved.\n')
                        else:
                            check_file = Path(str(destination)+'/'+str(items.name))
                            if destination.is_dir():
                                if not check_file.is_file():
                                    shutil.move(src=items, dst=destination)
                                    success_logs.append(f'Moving this file: {items.name} in {destination}.\n')
                                    file_moved = True
                                    countmoved += 1
                                else: issue_logs.append(f'File already exists in {destination}.\n')
                            else: issue_logs.append(f'Error moving {items.name}. {destination} is not a valid directory.\n')
                        break # Stop types loop if file extension is found
                if file_moved: break # Stop filetypes_list loop if file is moved

            if not file_moved :
                issue_logs.append(f'This file is not moved: {items.name}.\n')
                notmoved += 1

        summary_logs.append(f'Total of files to process: {len(files_list)}.\n')
        summary_logs.append(f'Total of files moved: {countmoved}.\n')
        summary_logs.append(f'Total of files not moved: {notmoved}.\n')

    except Exception as e:
        print(e)
        issue_logs.append(e)

    create_log(logs, src_directory)

def create_log(logs, src_directory):
    today = date.today().strftime('%m%d%Y')
    log_filename = str(src_directory)+'/file_organizer_logs_'+today+'.txt'

    try:
        with open(log_filename, 'w') as l_file:
            l_file.write(f'Organizing files in {src_directory}...\n',)
            l_file.write(f'Date: {date.today()}\n')
            for log_list in logs:
                if len(log_list) > 1:
                    l_file.write('\n'.join(log_list))

        print(f'Completed Moving files. Log file located at {log_filename}.\n')
    except Exception as e:
        print(e)

if __name__ == '__main__':

    try:
        full_path, for_review = get_file_path()
        if full_path is not None:
            if for_review: organize_files_for_review(full_path)
            elif not for_review: organize_files(full_path)

    except Exception as e:
        print(e)
   
