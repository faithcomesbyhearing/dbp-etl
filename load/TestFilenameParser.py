# TestFilenameParser.py

# Sample filenames for testing
test_audio_files = [
    "ENGESVN2DA_B01_MAT_001.mp3",   # Full chapter - should match audio99
    "IRUNLCP1DA_B13_1TH_001_001-001_010.opus",  # Partial chapter - should match audio100
    "SPANESV1DA_A23_GEN_005.webm",  # Full chapter - should match audio99
    "FRENCHL1DA_A32_ZEC_010_003-011_009.mp3",  # Partial chapter - should match audio100
    "B01___01_Matthew_____ENGGIDN2DA.mp3",  # Ordering format - should match audio101
    "A02___12_Exodus_____SPANESV1DA.opus",  # Ordering format - should match audio101
    "B11___123_Psalms___FRENCHL1DA.webm",  # Ordering format - should match audio101
    "B27___08_Revelation__ASMDPIN1DA.mp3",  # Ordering format - should match audio101
    "B27____08_Revelation__ASMDPIN1DA.mp3",  # Invalid case - more than 3 "_" after {A/B}{ordering}
    "ENGES_B01_MAT_001.mp3",  # Invalid case - should not match any
    "ENGES_B0_MAT_001.mp3",  # Invalid case - incorrect book number
    "ENGES_B01_MAT_00.mp3",  # Invalid case - incorrect chapter number
    "INVALID_FILENAME.mp3"  # Invalid case - should not match any
]

test_video_files = [
    "English_KJV_JHN_1-1-18.mp4",   # Full chapter - not match any
    "English-KJV_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English_KJV_JHN_1-1-18.mp4",   # Full chapter - not match any
    "English_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English-KJV-other-version-info_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English-other-version-info_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English other version info space_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English KJV other version info space_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "Tajik_WBT_LUK_End_Credits.mp4",   # Full chapter - not match any
    "English_KJV_JHN_End_credits.mp4",   # Full chapter - not match any
    "English-KJV-other-version-info_JHN_End_credits.mp4",   # Full chapter - should match video2
    "English-other-version-info_JHN_End_credits.mp4",   # Full chapter - should match video2
    "English-other_version-info_JHN_End_credits.mp4",   # Full chapter - not match any
    "English other version space_JHN_End_credits.mp4",   # Full chapter - should match video2
    "English KVJ other version space_JHN_End_credits.mp4",   # Full chapter - should match video2
    "Tajik-WBT_LUK_End_Credits.mp4",   # Full chapter - should match video2
    "English-KJV_JHN_End_credits.mp4",   # Full chapter - should match video2
    "English_JHN_End_Credits.mp4",   # Full chapter - should match video1
    "COVENANT_SEGMENT 01 – Intro and Garden of Eden.mp4",   # Full chapter - should match video5
    "COVENANT_SEGMENT 02 – The Fall.mp4",   # Full chapter - should match video5
    "COVENANT - Fall.mp4",  # Invalid case - should not match any
    "INVALID_FILENAME.mp4"  # Invalid case - should not match any
]

test_text_files = [
    "001GEN.usx",   # Full chapter - should match text3
    "040MAT.usx",   # Full chapter - should match text3
    "041MRK.usx",   # Full chapter - should match text3
    "001GEN_001-010.usx",  # Invalid case - should not match any
    "INVALID_FILENAME.usx"  # Invalid case - should not match any
]

test_cases_parser = [
    # Valid cases
    ("001GEN.usx", "GEN", "1", "1", "", "usx"),
    ("ENGESVN2DA_B01_MAT_001.mp3", "MAT", "001", "1", "", "mp3"),
    ("IRUNLCP1DA_B13_1TH_001_001-001_010.opus", "1TH", "001", "001", "010", "opus"),
    ("English-other-version-info_JHN_End_credits.mp4", "JHN", "end", "", "", "mp4"),
    ("English-other version info space_JHN_1-1-18.mp4", "JHN", "1", "1", "18", "mp4"),
    ("English-KJV_JHN_1-1-18.mp4", "JHN", "1", "1", "18", "mp4"),
    ("English-KJV_JHN_End_credits.mp4", "JHN", "end", "", "", "mp4"),
    ("COVENANT_SEGMENT 01 – Intro and Garden of Eden.mp4", "C01", "1", "1", "1", "mp4"),
    # Invalid cases
    ("001GEN_001-010.usx", "", "", "", "", ""),
    ("INVALID_FILENAME.mp3", "", "", "", "", ""),
    ("ENGES_B01_MAT_00.mp3", "", "", "", "", ""),
]

def validate_files(files, templates, file_type):
    success_count = 0
    failure_count = 0

    print("\n=====================================")
    print("File Type : ", file_type)
    print("=====================================")

    for file in files:
        matched = False
        for template in templates:
            match = template.regex.match(file)
            if match:
                print(f"✅ Valid: {file} (Matched: {template.name})")
                for idx, group in enumerate(match.groups(), start=1):
                    print(f"   - Group {idx}: {group}")
                matched = True
                success_count += 1
                break
        
        if not matched:
            print(f"❌ Invalid: {file} (No pattern matched)")
            failure_count += 1

    return success_count, failure_count

def test_parse_method(parser, test_cases):
    success_count = 0
    failure_count = 0

    print("\n=====================================")
    print("Testing parse method of FilenameParser")
    print("=====================================")

    for filename, expected_bookId, expected_chapter, expected_verseStart, expected_verseEnd, expected_type in test_cases:
        # Wrap the filename in a tuple as expected by the parser
        filename_tuple = (filename, len(filename), None)

        # Parse the filename
        file = parser.parseOneFilename3(parser.audioTemplates + parser.videoTemplates + parser.textTemplates, "", filename_tuple)

        # Validate the results
        if file.bookId == expected_bookId and \
           file.chapter == expected_chapter and \
           file.verseStart == expected_verseStart and \
           file.verseEnd == expected_verseEnd and \
           file.type == expected_type:
            success_count += 1
            print(f"✅ Passed: {filename}")
        else:
            print(f"❌ Failed: {filename}")
            print(f"   - Expected: bookId={expected_bookId}, chapter={expected_chapter}, verseStart={expected_verseStart}, verseEnd={expected_verseEnd}, type={expected_type}")
            print(f"   - Got: bookId={file.bookId}, chapter={file.chapter}, verseStart={file.verseStart}, verseEnd={file.verseEnd}, type={file.type}")
            failure_count += 1

    return success_count, failure_count

if __name__ == '__main__':
    from FilenameParser import FilenameParser
    from Config import Config
    config = Config()
    parser = FilenameParser(config)
    parser.chapterMap = {
        "GEN": 50,
        "MAT": 28,
        "MRK": 16,
        "JHN": 21,
        "C01": 2,
    }
    parser.maxChapterMap = {
        "GEN": 50,
        "MAT": 28,
        "MRK": 16,
        "JHN": 21,
        "C01": 2,
    }
    parser.covenantBookNameMap = {
        "C01": "Intro & Garden of Eden",
        "C02": "Noah, Abram, Ishmael is Born",
    }
    audio_success, audio_failure = validate_files(test_audio_files, parser.audioTemplates, "audio")
    video_success, video_failure = validate_files(test_video_files, parser.videoTemplates, "video")
    text_success, text_failure = validate_files(test_text_files, parser.textTemplates, "text")

    total_success = audio_success + video_success + text_success
    total_failure = audio_failure + video_failure + text_failure

    parse_success, parse_failure = test_parse_method(parser, test_cases_parser)
    total_success += parse_success
    total_failure += parse_failure
    print("\nSummary:")

    # Validation check
    expected_success_count = 37
    expected_failure_count = 14

    if total_success == expected_success_count and total_failure == expected_failure_count:
        print("✅ Test executed successfully.")
    else:
        print("❌ Test failed.")
