import unittest
import os
from MetadataReader import MetadataReader
from Config import *

class TestMetadataReader(unittest.TestCase):
    def setUp(self):
        config = Config()
        self.metadataReader = MetadataReader(config.filename_metadata_xml)

    def test_get_metadata_records(self):
        records = self.metadataReader.getResultSet()
        self.assertEqual(len(records) > 0, True, "Count records [%s]" % len(records))

    def test_get_metadata_record_by_name(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        self.assertEqual(record.Name(), 'Indian Revised Version (IRV) Tamil - 2019', "Name for Indian Revised Version (IRV) Tamil - 2019 should be [%s]" % record.Name())

    def test_check_language(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        language = record.Language()

        self.assertEqual(language.get("iso"), 'tam', "Language for Indian Revised Version (IRV) Tamil - 2019 should be [%s]" % language.get("iso"))

    def test_check_rights_holder(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        rightsHolder = record.RightsHolder()
        self.assertEqual(rightsHolder.get('abbr'), 'BCS', "The abbr column that belongs to the Rights Holder Map for should be [%s]" % 'BCS')

    def test_check_book_names(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        bookNames = record.BookNames()

        for book in bookNames:
            for key, record in book.items():
                if key == 'book-2co':
                    self.assertEqual(book.get(key), {'abbr': '2கொரி', 'short': '2 கொரி', 'long': '2 கொரிந்தியர்'}, "The book: book-2co should be [%s]" % "{'abbr': '2கொரி', 'short': '2 கொரி', 'long': '2 கொரிந்தியர்'}")

    def test_get_book_names_by_name(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        bookName = record.getBookNameByName('book-2co')

        self.assertEqual(bookName.get("abbr"), '2கொரி', "The book abbr should be [%s]" % "2கொரி")
        self.assertEqual(bookName.get("short"), '2 கொரி', "The book short should be [%s]" % "2 கொரி")
       

    def test_check_book_order(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        bookOrder = record.BookOrder()
        for book in bookOrder:
            for key, value in book.items():
                if key == 'name' and value == 'book-2co':
                    self.assertEqual(book.get('role'), '2CO', "The book ORDER: book-2co should be [%s]" % "2CO")

    def test_check_attributes(self):
        record = self.metadataReader.getMetadataRecordByName('Indian Revised Version (IRV) Tamil - 2019')
        listAttributes = [
            'identification',
            'type',
            'relationships',
            'agencies',
            'language',
            'countries',
            'format',
            'names',
            'manifest',
            'source',
            'publications',
            'copyright',
            'promotion',
            'archiveStatus'
        ]
        attributes = record.getAttributes()
        self.assertEqual(attributes, listAttributes, "The Attributes list should be [%s]" % "identification, type, relationships, agencies, language, countries, format, names, manifest, source, publications, copyright, promotion, archiveStatus")

if __name__ == '__main__':
    unittest.main(argv=['test'], exit=False)
