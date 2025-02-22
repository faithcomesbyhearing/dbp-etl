# TestFilenameParser.py

# Sample filenames for testing
test_audio_files = [
    "ENGESVN2DA_B01_MAT_001.mp3",   # Full chapter - should match audio99
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
    "English_KJV_JHN_1-1-18.mp4",   # Full chapter - should match video1
    "English_KJV_JHN_End_credits.mp4",   # Full chapter - should match video2
    "Tajik_WBT_LUK_End_Credits.mp4",   # Full chapter - should match video2
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

if __name__ == '__main__':
    from FilenameParser import FilenameParser
    from Config import Config
    config = Config()
    parser = FilenameParser(config)

    audio_success, audio_failure = validate_files(test_audio_files, parser.audioTemplates, "audio")
    video_success, video_failure = validate_files(test_video_files, parser.videoTemplates, "video")
    text_success, text_failure = validate_files(test_text_files, parser.textTemplates, "text")

    total_success = audio_success + video_success + text_success
    total_failure = audio_failure + video_failure + text_failure

    print("\nSummary:")

    # Validation check
    expected_success_count = 17
    expected_failure_count = 9

    if total_success == expected_success_count and total_failure == expected_failure_count:
        print("✅ Test executed successfully.")
    else:
        print("❌ Test failed.")
