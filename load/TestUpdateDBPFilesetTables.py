import unittest
import os
import tempfile
import csv
from unittest.mock import Mock, patch, MagicMock
from Config import Config
from UpdateDBPFilesetTables import UpdateDBPFilesetTables
from InputFileset import InputFileset
from SQLUtility import SQLUtility
from SQLBatchExec import SQLBatchExec


class TestUpdateDBPFilesetTables(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.config = Mock(spec=Config)
        self.config.s3_bucket = "test-bucket"
        self.config.s3_vid_bucket = "test-video-bucket"
        
        # Add additional config attributes required by UpdateDBPBibleFilesSecondary
        self.config.s3_zipper_user_key = "test-zipper-key"
        self.config.s3_zipper_user_secret = "test-zipper-secret"
        self.config.s3_aws_region = "us-east-1"
        self.config.biblebrain_services_base_url = "https://test.biblebrain.com"
        
        self.db = Mock(spec=SQLUtility)
        self.dbOut = Mock(spec=SQLBatchExec)
        self.languageReader = Mock()
        
        # Mock the database sets for OT and NT books
        def mock_select_set(query, _):
            if query == "SELECT id FROM books WHERE book_testament = 'OT'":
                return {"GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", "EZR", "NEH", "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", "JOL", "AMO", "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL"}
            elif query == "SELECT id FROM books WHERE book_testament = 'NT'":
                return {"MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH", "PHP", "COL", "1TH", "2TH", "1TI", "2TI", "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV"}
            return set()
        
        self.db.selectSet.side_effect = mock_select_set
        
        # Use patch.object to mock the AWSSession and S3ZipperService within the initialization context
        with patch('UpdateDBPBibleFilesSecondary.AWSSession') as mock_aws_session, \
             patch('UpdateDBPBibleFilesSecondary.S3ZipperService') as mock_s3_zipper, \
             patch('UpdateDBPBibleFilesSecondary.BibleBrainService') as mock_bible_brain:
            mock_aws_session.shared.return_value.s3Client = Mock()
            mock_s3_zipper.return_value = Mock()
            mock_bible_brain.return_value = Mock()
            self.updater = UpdateDBPFilesetTables(self.config, self.db, self.dbOut, self.languageReader)

    def test_map_files_by_book_chapter_basic(self):
        """Test map_files_by_book_chapter with basic CSV data."""
        # Create a temporary CSV file
        csv_data = [
            {"book_id": "MAT", "chapter_start": "1", "chapter_end": "1", "verse_start": "1", "verse_end": "25", "verse_sequence": "1", "file_name": "MAT01_01.mp4", "file_size": "1000000"},
            {"book_id": "MAT", "chapter_start": "1", "chapter_end": "1", "verse_start": "10", "verse_end": "25", "verse_sequence": "10", "file_name": "MAT01_10.mp4", "file_size": "1000000"},
            {"book_id": "MAT", "chapter_start": "2", "chapter_end": "2", "verse_start": "1", "verse_end": "23", "verse_sequence": "1", "file_name": "MAT02_01.mp4", "file_size": "1000000"},
            {"book_id": "MRK", "chapter_start": "1", "chapter_end": "1", "verse_start": "1", "verse_end": "45", "verse_sequence": "1", "file_name": "MRK01_01.mp4", "file_size": "1500000"},
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            writer = csv.DictWriter(temp_file, fieldnames=["book_id", "chapter_start", "chapter_end", "verse_start", "verse_end", "verse_sequence", "file_name", "file_size"])
            writer.writeheader()
            writer.writerows(csv_data)
            temp_file_path = temp_file.name

        try:
            # Mock input fileset
            input_fileset = Mock(spec=InputFileset)
            input_fileset.csvFilename = temp_file_path
            
            # Test the method
            result = self.updater.map_files_by_book_chapter(input_fileset)
            
            # Verify the results
            expected_keys = [
                ("MAT", 1, "_stream.m3u8"),
                ("MAT", 1, "_web.mp4"),
                ("MAT", 1, ".mp4"),
                ("MAT", 2, "_stream.m3u8"),
                ("MAT", 2, "_web.mp4"),
                ("MAT", 2, ".mp4"),
                ("MRK", 1, "_stream.m3u8"),
                ("MRK", 1, "_web.mp4"),
                ("MRK", 1, ".mp4"),
            ]
            
            for key in expected_keys:
                self.assertIn(key, result, f"Expected key {key} not found in result")
            
            # Check specific values
            self.assertEqual(len(result[("MAT", 1, "_stream.m3u8")]), 2, "MAT chapter 1 should have 2 files for _stream.m3u8 suffix")
            self.assertEqual(len(result[("MAT", 2, "_stream.m3u8")]), 1, "MAT chapter 2 should have 1 file for _stream.m3u8 suffix")
            self.assertEqual(len(result[("MRK", 1, "_stream.m3u8")]), 1, "MRK chapter 1 should have 1 file for _stream.m3u8 suffix")
            
            # Verify the base filenames are stored correctly
            self.assertIn("MAT01_01.mp4", result[("MAT", 1, "_stream.m3u8")])
            self.assertIn("MAT01_10.mp4", result[("MAT", 1, "_stream.m3u8")])
            
        finally:
            os.unlink(temp_file_path)

    def test_map_files_by_book_chapter_single_file_per_chapter(self):
        """Test map_files_by_book_chapter with one file per chapter."""
        csv_data = [
            {"book_id": "MAT", "chapter_start": "1", "chapter_end": "1", "verse_start": "1", "verse_end": "25", "verse_sequence": "1", "file_name": "MAT01.mp4", "file_size": "1000000"},
            {"book_id": "MAT", "chapter_start": "2", "chapter_end": "2", "verse_start": "1", "verse_end": "23", "verse_sequence": "1", "file_name": "MAT02.mp4", "file_size": "1000000"},
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            writer = csv.DictWriter(temp_file, fieldnames=["book_id", "chapter_start", "chapter_end", "verse_start", "verse_end", "verse_sequence", "file_name", "file_size"])
            writer.writeheader()
            writer.writerows(csv_data)
            temp_file_path = temp_file.name

        try:
            input_fileset = Mock(spec=InputFileset)
            input_fileset.csvFilename = temp_file_path
            
            result = self.updater.map_files_by_book_chapter(input_fileset)
            
            # Each chapter should have exactly 1 file for each suffix variant
            for suffix, _ in InputFileset.VIDEO_VARIANTS:
                self.assertEqual(len(result[("MAT", 1, suffix)]), 1, f"MAT chapter 1 should have 1 file for {suffix} suffix")
                self.assertEqual(len(result[("MAT", 2, suffix)]), 1, f"MAT chapter 2 should have 1 file for {suffix} suffix")
                
        finally:
            os.unlink(temp_file_path)

    def test_map_files_by_book_chapter_empty_csv(self):
        """Test map_files_by_book_chapter with empty CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            writer = csv.DictWriter(temp_file, fieldnames=["book_id", "chapter_start", "chapter_end", "verse_start", "verse_end", "verse_sequence", "file_name", "file_size"])
            writer.writeheader()
            temp_file_path = temp_file.name

        try:
            input_fileset = Mock(spec=InputFileset)
            input_fileset.csvFilename = temp_file_path
            
            result = self.updater.map_files_by_book_chapter(input_fileset)
            
            self.assertEqual(result, {}, "Empty CSV should result in empty mapping")
            
        finally:
            os.unlink(temp_file_path)

    @patch('builtins.open')
    def test_handle_input_files_for_video_chapter_complete_logic(self, _):
        """Test the chapter complete logic in handle_input_files_for_video method."""
        # Mock CSV data
        csv_content = """book_id,chapter_start,verse_start,chapter_end,verse_end,file_name,file_size,verse_sequence
MAT,1,1,1,25,MAT01.mp4,1000000,1
MAT,2,1,2,23,MAT02_01.mp4,1000000,1
MAT,2,10,2,23,MAT02_10.mp4,1000000,10
MRK,1,1,1,45,MRK01.mp4,1500000,1"""
        
        mock_file = MagicMock()
        mock_file.__enter__.return_value = csv_content.splitlines()
        mock_file.__iter__ = lambda self: iter(csv_content.splitlines())
        open.return_value = mock_file
        
        # Mock csv.DictReader
        with patch('csv.DictReader') as mock_csv_reader:
            mock_csv_reader.return_value = [
                {"book_id": "MAT", "chapter_start": "1", "verse_start": "1", "chapter_end": "1", "verse_end": "25", "file_name": "MAT01.mp4", "file_size": "1000000", "verse_sequence": "1"},
                {"book_id": "MAT", "chapter_start": "2", "verse_start": "1", "chapter_end": "2", "verse_end": "23", "file_name": "MAT02_01.mp4", "file_size": "1000000", "verse_sequence": "1"},
                {"book_id": "MAT", "chapter_start": "2", "verse_start": "10", "chapter_end": "2", "verse_end": "23", "file_name": "MAT02_10.mp4", "file_size": "1000000", "verse_sequence": "10"},
                {"book_id": "MRK", "chapter_start": "1", "verse_start": "1", "chapter_end": "1", "verse_end": "45", "file_name": "MRK01.mp4", "file_size": "1500000", "verse_sequence": "1"},
            ]
            
            # Mock S3Utility
            with patch('UpdateDBPFilesetTables.S3Utility') as mock_s3_class:
                mock_s3_instance = Mock()
                mock_s3_instance.get_key_info.return_value = (True, 1000000)  # exists=True, size=1000000
                mock_s3_class.return_value = mock_s3_instance
                
                # Mock input fileset
                input_fileset = Mock(spec=InputFileset)
                input_fileset.csvFilename = "test.csv"
                input_fileset.filesetPrefix = "video/TESTBIBLE/TESTFILESET"
                input_fileset.getInputFile.return_value = Mock(duration=120.5)
                
                # Mock existing rows (empty for this test)
                existing_rows = []
                hash_id = "test_hash_id"
                
                # Call the method
                inserts, updates, deletes = self.updater.handle_input_files_for_video(existing_rows, hash_id, input_fileset)
                
                # Verify inserts were created
                self.assertTrue(len(inserts) > 0, "Should have insert statements")
                
                # Check chapter complete logic
                # MAT chapter 1 has 1 file per suffix variant -> should be complete (is_complete_chapter=1)
                # MAT chapter 2 has 2 files per suffix variant -> should not be complete (is_complete_chapter=0)
                # MRK chapter 1 has 1 file per suffix variant -> should be complete (is_complete_chapter=1)
                
                mat_ch1_complete_count = 0
                mat_ch2_incomplete_count = 0
                mrk_ch1_complete_count = 0
                
                for insert in inserts:
                    # Insert format: (c_end, v_end, variant_file_size, duration, v_seq, is_complete_chapter,
                    #                 hash_id, book_id, c_start, v_start, filename)
                    is_complete_chapter = insert[5]
                    book_id = insert[7]
                    chapter_start = insert[8]
                    
                    if book_id == "MAT" and chapter_start == 1:
                        if is_complete_chapter == 1:
                            mat_ch1_complete_count += 1
                    elif book_id == "MAT" and chapter_start == 2:
                        if is_complete_chapter == 0:
                            mat_ch2_incomplete_count += 1
                    elif book_id == "MRK" and chapter_start == 1:
                        if is_complete_chapter == 1:
                            mrk_ch1_complete_count += 1
                
                # MAT chapter 1 should be complete for all 3 video variants
                self.assertEqual(mat_ch1_complete_count, 3, "MAT chapter 1 should be complete (3 video variants)")
                
                # MAT chapter 2 should be incomplete for all 3 video variants (has 2 files)
                self.assertEqual(mat_ch2_incomplete_count, 6, "MAT chapter 2 should be incomplete (2 files × 3 video variants)")
                
                # MRK chapter 1 should be complete for all 3 video variants
                self.assertEqual(mrk_ch1_complete_count, 3, "MRK chapter 1 should be complete (3 video variants)")

    @patch('builtins.open')
    def test_handle_input_files_for_video_end_chapter_logic(self, _):
        """Test the chapter complete logic for 'end' chapters in handle_input_files_for_video method."""
        # Mock CSV data with "end" chapter
        with patch('csv.DictReader') as mock_csv_reader:
            mock_csv_reader.return_value = [
                {"book_id": "MAT", "chapter_start": "end", "verse_start": "1", "chapter_end": "28", "verse_end": "20", "file_name": "MAT_end.mp4", "file_size": "1000000", "verse_sequence": "1"},
            ]
            
            # Mock S3Utility
            with patch('UpdateDBPFilesetTables.S3Utility') as mock_s3_class:
                mock_s3_instance = Mock()
                mock_s3_instance.get_key_info.return_value = (True, 1000000)
                mock_s3_class.return_value = mock_s3_instance
                
                # Mock input fileset
                input_fileset = Mock(spec=InputFileset)
                input_fileset.csvFilename = "test.csv"
                input_fileset.filesetPrefix = "video/TESTBIBLE/TESTFILESET"
                input_fileset.getInputFile.return_value = Mock(duration=120.5)
                
                # Mock existing rows
                existing_rows = []
                hash_id = "test_hash_id"
                
                # Call the method
                inserts, updates, deletes = self.updater.handle_input_files_for_video(existing_rows, hash_id, input_fileset)
                
                # Verify that "end" chapters are never marked as complete
                end_chapter_complete_count = 0
                
                for insert in inserts:
                    is_complete_chapter = insert[5]
                    if is_complete_chapter == 1:
                        end_chapter_complete_count += 1
                
                # "end" chapters should never be marked as complete
                self.assertEqual(end_chapter_complete_count, 0, "'end' chapters should never be marked as complete")

    def test_parse_chapter_verse_normal_cases(self):
        """Test parse_chapter_verse method with normal cases."""
        # Test normal chapter/verse
        row = {
            "book_id": "MAT",
            "chapter_start": "1",
            "chapter_end": "1", 
            "verse_start": "10",
            "verse_end": "15",
            "verse_sequence": "10",
            "file_size": "1000000"
        }
        
        c_start, c_end, v_start, v_seq, v_end, f_size = self.updater.parse_chapter_verse(row)
        
        self.assertEqual(c_start, 1)
        self.assertEqual(c_end, 1)
        self.assertEqual(v_start, "10")
        self.assertEqual(v_seq, 10)
        self.assertEqual(v_end, "15")
        self.assertEqual(f_size, 1000000)

    def test_parse_chapter_verse_end_case(self):
        """Test parse_chapter_verse method with 'end' chapter case."""
        row = {
            "book_id": "MAT",
            "chapter_start": "end",
            "chapter_end": "28",
            "verse_start": "15",
            "verse_end": "20",
            "verse_sequence": "15",
            "file_size": "1000000"
        }
        
        c_start, c_end, v_start, v_seq, v_end, f_size = self.updater.parse_chapter_verse(row)
        
        # For MAT, convertChapterStart should return (28, "21", "21")
        self.assertEqual(c_start, 28)
        self.assertEqual(c_end, 28)
        self.assertEqual(v_start, "21")  # Should be converted to the end verse
        self.assertEqual(v_seq, 21)      # Should be max of verse_sequence and verse_start_end_chap
        self.assertEqual(v_end, "21")
        self.assertEqual(f_size, 1000000)

    def test_parse_chapter_verse_missing_values(self):
        """Test parse_chapter_verse method with missing/empty values."""
        row = {
            "book_id": "JHN",
            "chapter_start": "1",
            "chapter_end": "",
            "verse_start": "",
            "verse_end": "",
            "verse_sequence": "",
            "file_size": ""
        }
        
        c_start, c_end, v_start, v_seq, v_end, f_size = self.updater.parse_chapter_verse(row)
        
        self.assertEqual(c_start, 1)
        self.assertEqual(c_end, None)    # Empty string should become None
        self.assertEqual(v_start, "1")   # Should default to "1"
        self.assertEqual(v_seq, 1)       # Should default to 1
        self.assertEqual(v_end, None)    # Empty string should become None
        self.assertEqual(f_size, None)   # Empty string should become None


if __name__ == '__main__':
    unittest.main(argv=['test'], exit=False)