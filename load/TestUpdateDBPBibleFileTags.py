import unittest
import os
# from LPTSExtractReader import LPTSExtractReader
# from StageCLanguageReader import StageCLanguageReader
from Config import *
from UpdateDBPBibleFileTags import UpdateDBPBibleFileTags
from SQLUtility import *
from SQLBatchExec import *

class TestLanguageReaderStage(unittest.TestCase):
    def setUp(self):
        config = Config.shared()
        db = SQLUtility(config)
        dbOut = SQLBatchExec(config)
        self.updateService = UpdateDBPBibleFileTags(config, db, dbOut)

    def test_find_verse_closest(self):
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 3, 5, 8, 10, 12, 16], 7), 8, "Closest number should be [%d]" % 8)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 3, 5, 8, 10, 13, 16], 9), 10, "Closest number should be [%d]" % 10)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 3, 8, 9, 10, 13, 16], 7), 8, "Closest number should be [%d]" % 8)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([2, 3, 4, 9, 10, 13, 16], 7), 9, "Closest number should be [%d]" % 9)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([3, 3, 4, 8, 9], 7), 8, "Closest number should be [%d]" % 8)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 3], 2), 3, "Closest number should be [%d]" % 3)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 2, 3], 2), 2, "Closest number should be [%d]" % 2)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 2, 3], 18), 3, "Closest number should be [%d]" % 2)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 5, 8, 8, 9, 9, 9, 10, 10, 13, 16, 455, 544], 544), 544, "Closest number should be [%d]" % 544)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([1, 2, 8, 21, 110, 113, 116], 7), 8, "Closest number should be [%d]" % 8)
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([], None), None, "Closest number should be [%s]" % 'None')
        self.assertEqual(UpdateDBPBibleFileTags.findVerseClosest([], 2), None, "Closest number should be [%s]" % 'None')
    
    def test_get_thumbail_name_from_dict(self):
        thumbnails = {
            "mat": {
                1: {
                    1:"gf_mat_01_01.jpg", 20:"gf_mat_01_20.jpg"
                },
                2: {
                    2:"gf_mat_02_02.jpg", 13:"gf_mat_02_13.jpg"
                },
                3: {
                    13:"gf_mat_03_13.jpg"
                },
                4: {
                    1:"gf_mat_04_01.jpg", 21:"gf_mat_04_21.jpg"
                },
                5: {
                    13:"gf_mat_05_13.jpg", 27:"gf_mat_05_27.jpg"
                },
                6:{
                    2:"gf_mat_06_02.jpg", 26:"gf_mat_06_26.jpg"
                },
                7:{
                    3:"gf_mat_07_03.jpg", 24:"gf_mat_07_24.jpg"
                },
                8:{
                    4:"gf_mat_08_04.jpg", 28:"gf_mat_08_28.jpg"
                },
                9:{
                    2:"gf_mat_09_02.jpg", 18:"gf_mat_09_18.jpg"
                },
                10:{
                    1:"gf_mat_10_01.jpg", 24:"gf_mat_10_24.jpg"
                },
                11:{
                    3:"gf_mat_11_03.jpg", 28:"gf_mat_11_28.jpg"
                },
                12:{
                    2:"gf_mat_12_02.jpg", 22:"gf_mat_12_22.jpg", 46:"gf_mat_12_46.jpg"
                },
                13:{
                    2:"gf_mat_13_02.jpg", 32:"gf_mat_13_32.jpg", 44:"gf_mat_13_44.jpg"
                },
                14:{
                    3:"gf_mat_14_03.jpg", 25:"gf_mat_14_25.jpg"
                },
                15:{
                    2:"gf_mat_15_02.jpg", 22:"gf_mat_15_22.jpg"
                },
                16:{
                    1:"gf_mat_16_01.jpg", 16:"gf_mat_16_16.jpg"
                },
            }
        }

        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnails, "mat", 1, 1), 'gf_mat_01_01.jpg', "Thumbnail file name should be [%s]" % 'gf_mat_01_01.jpg')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnails, "mat", 2, 13), 'gf_mat_02_13.jpg', "Thumbnail file name should be [%s]" % 'gf_mat_02_13.jpg')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnails, "mat", 2, 12), 'gf_mat_02_13.jpg', "Thumbnail file name should be [%s]" % 'gf_mat_02_13.jpg')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnails, "mat", 15, 17), 'gf_mat_15_22.jpg', "Thumbnail file name should be [%s]" % 'gf_mat_15_22.jpg')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnails, "mat", 100, 17), None, "Thumbnail file name should be [%s]" % 'None')
        self.assertEqual(self.updateService.getThumbnailNameFromDict({}, "mat", 100, 17), None, "Thumbnail file name should be [%s]" % 'None')

    def test_get_thumbnail_files_dict_indexed_book_chapter_verse(self):
        thumbnailsListFileName = ['gf_credits.jpg', 'gf_jhn_01_02.jpg', 'gf_jhn_01_19.jpg', 'gf_jhn_01_38.jpg', 'gf_jhn_02_09.jpg', 'gf_jhn_02_15.jpg', 'gf_jhn_03_02.jpg', 'gf_jhn_03_23.jpg', 'gf_jhn_04_07.jpg', 'gf_jhn_04_46.jpg', 'gf_jhn_05_06.jpg', 'gf_jhn_05_19.jpg', 'gf_jhn_06_11.jpg', 'gf_jhn_06_19.jpg', 'gf_jhn_06_25.jpg', 'gf_jhn_06_61.jpg', 'gf_jhn_07_02.jpg', 'gf_jhn_07_30.jpg', 'gf_jhn_08_03.jpg', 'gf_jhn_08_22.jpg', 'gf_jhn_08_39.jpg', 'gf_jhn_09_02.jpg', 'gf_jhn_09_26.jpg', 'gf_jhn_10_11.jpg', 'gf_jhn_10_31.jpg', 'gf_jhn_11_01.jpg', 'gf_jhn_11_32.jpg', 'gf_jhn_11_47.jpg', 'gf_jhn_12_03.jpg', 'gf_jhn_12_23.jpg', 'gf_jhn_12_46.jpg', 'gf_jhn_13_05.jpg', 'gf_jhn_13_25.jpg', 'gf_jhn_14_06.jpg', 'gf_jhn_14_15.jpg', 'gf_jhn_15_01.jpg', 'gf_jhn_15_18.jpg', 'gf_jhn_16_01.jpg', 'gf_jhn_16_19.jpg', 'gf_jhn_17_05.jpg', 'gf_jhn_17_24.jpg', 'gf_jhn_18_03.jpg', 'gf_jhn_18_33.jpg', 'gf_jhn_19_04.jpg', 'gf_jhn_19_19.jpg', 'gf_jhn_19_38.jpg', 'gf_jhn_20_06.jpg', 'gf_jhn_20_27.jpg', 'gf_jhn_21_03.jpg', 'gf_jhn_21_15.jpg', 'gf_luk_01_01.jpg', 'gf_luk_01_24.jpg', 'gf_luk_01_27.jpg', 'gf_luk_01_57.jpg', 'gf_luk_02_06.jpg', 'gf_luk_02_28.jpg', 'gf_luk_02_46.jpg', 'gf_luk_03_03.jpg', 'gf_luk_03_21.jpg', 'gf_luk_04_01.jpg', 'gf_luk_04_16.jpg', 'gf_luk_04_33.jpg', 'gf_luk_05_12.jpg', 'gf_luk_05_19.jpg', 'gf_luk_05_29.jpg', 'gf_luk_06_02.jpg', 'gf_luk_06_18.jpg', 'gf_luk_06_39.jpg', 'gf_luk_07_02.jpg', 'gf_luk_07_19.jpg', 'gf_luk_07_38.jpg', 'gf_luk_08_01.jpg', 'gf_luk_08_22.jpg', 'gf_luk_08_47.jpg', 'gf_luk_09_02.jpg', 'gf_luk_09_13.jpg', 'gf_luk_09_28.jpg', 'gf_luk_09_57.jpg', 'gf_luk_10_03.jpg', 'gf_luk_10_30.jpg', 'gf_luk_11_02.jpg', 'gf_luk_11_37.jpg', 'gf_luk_12_01.jpg', 'gf_luk_12_27.jpg', 'gf_luk_12_35.jpg', 'gf_luk_12_51.jpg', 'gf_luk_13_06.jpg', 'gf_luk_13_24.jpg', 'gf_luk_14_01.jpg', 'gf_luk_14_26.jpg', 'gf_luk_15_03.jpg', 'gf_luk_15_11.jpg', 'gf_luk_16_01.jpg', 'gf_luk_16_20.jpg', 'gf_luk_17_12.jpg', 'gf_luk_17_22.jpg', 'gf_luk_18_03.jpg', 'gf_luk_18_18.jpg', 'gf_luk_19_04.jpg', 'gf_luk_19_37.jpg', 'gf_luk_20_02.jpg', 'gf_luk_20_27.jpg', 'gf_luk_21_01.jpg', 'gf_luk_21_34.jpg', 'gf_luk_22_05.jpg', 'gf_luk_22_14.jpg', 'gf_luk_22_39.jpg', 'gf_luk_22_55.jpg', 'gf_luk_23_01.jpg', 'gf_luk_23_26.jpg', 'gf_luk_23_53.jpg', 'gf_luk_24_15.jpg', 'gf_luk_24_51.jpg', 'gf_mat_01_01.jpg', 'gf_mat_01_20.jpg', 'gf_mat_02_02.jpg', 'gf_mat_02_13.jpg', 'gf_mat_03_13.jpg', 'gf_mat_04_01.jpg', 'gf_mat_04_21.jpg', 'gf_mat_05_13.jpg', 'gf_mat_05_27.jpg', 'gf_mat_06_02.jpg', 'gf_mat_06_26.jpg', 'gf_mat_07_03.jpg', 'gf_mat_07_24.jpg', 'gf_mat_08_04.jpg', 'gf_mat_08_28.jpg', 'gf_mat_09_02.jpg', 'gf_mat_09_18.jpg', 'gf_mat_10_01.jpg', 'gf_mat_10_24.jpg', 'gf_mat_11_03.jpg', 'gf_mat_11_28.jpg', 'gf_mat_12_02.jpg', 'gf_mat_12_22.jpg', 'gf_mat_12_46.jpg', 'gf_mat_13_02.jpg', 'gf_mat_13_32.jpg', 'gf_mat_13_44.jpg', 'gf_mat_14_03.jpg', 'gf_mat_14_25.jpg', 'gf_mat_15_02.jpg', 'gf_mat_15_22.jpg', 'gf_mat_16_01.jpg', 'gf_mat_16_16.jpg', 'gf_mat_17_02.jpg', 'gf_mat_17_25.jpg', 'gf_mat_18_01.jpg', 'gf_mat_18_21.jpg', 'gf_mat_19_02.jpg', 'gf_mat_19_16.jpg', 'gf_mat_20_08.jpg', 'gf_mat_20_34.jpg', 'gf_mat_21_09.jpg', 'gf_mat_21_12.jpg', 'gf_mat_21_34.jpg', 'gf_mat_22_02.jpg', 'gf_mat_22_17.jpg', 'gf_mat_22_24.jpg', 'gf_mat_23_04.jpg', 'gf_mat_23_15.jpg', 'gf_mat_23_33.jpg', 'gf_mat_24_03.jpg', 'gf_mat_24_32.jpg', 'gf_mat_24_41.jpg', 'gf_mat_25_01.jpg', 'gf_mat_25_15.jpg', 'gf_mat_25_33.jpg', 'gf_mat_26_07.jpg', 'gf_mat_26_33.jpg', 'gf_mat_26_59.jpg', 'gf_mat_27_02.jpg', 'gf_mat_27_11.jpg', 'gf_mat_27_35.jpg', 'gf_mat_27_65.jpg', 'gf_mat_28_01.jpg', 'gf_mat_28_16.jpg', 'gf_mrk_01_08.jpg', 'gf_mrk_01_24.jpg', 'gf_mrk_01_41.jpg', 'gf_mrk_02_11.jpg', 'gf_mrk_02_22.jpg', 'gf_mrk_02_23.jpg', 'gf_mrk_03_05.jpg', 'gf_mrk_03_23.jpg', 'gf_mrk_04_03.jpg', 'gf_mrk_04_39.jpg', 'gf_mrk_05_06.jpg', 'gf_mrk_05_22.jpg', 'gf_mrk_05_41.jpg', 'gf_mrk_06_02.jpg', 'gf_mrk_06_26.jpg', 'gf_mrk_06_27.jpg', 'gf_mrk_06_41.jpg', 'gf_mrk_06_45.jpg', 'gf_mrk_07_01.jpg', 'gf_mrk_07_26.jpg', 'gf_mrk_08_06.jpg', 'gf_mrk_08_23.jpg', 'gf_mrk_09_03.jpg', 'gf_mrk_09_27.jpg', 'gf_mrk_09_36.jpg', 'gf_mrk_10_14.jpg', 'gf_mrk_10_20.jpg', 'gf_mrk_10_50.jpg', 'gf_mrk_11_08.jpg', 'gf_mrk_11_27.jpg', 'gf_mrk_12_16.jpg', 'gf_mrk_12_19.jpg', 'gf_mrk_12_42.jpg', 'gf_mrk_13_02.jpg', 'gf_mrk_13_34.jpg', 'gf_mrk_14_03.jpg', 'gf_mrk_14_22.jpg', 'gf_mrk_14_32.jpg', 'gf_mrk_14_60.jpg', 'gf_mrk_14_69.jpg', 'gf_mrk_15_02.jpg', 'gf_mrk_15_20.jpg', 'gf_mrk_15_37.jpg', 'gf_mrk_15_46.jpg', 'gf_mrk_16_04.jpg']

        thumbnailsDict = self.updateService.getThumbnailFilesDictIndexedBookChapterVerse(thumbnailsListFileName)

        self.assertEqual(thumbnailsDict.get('mrk').get(8).get(6), 'gf_mrk_08_06.jpg', "Thumbnail file name from dict should be [%s]" % 'gf_mrk_08_06.jpg')
        self.assertEqual(thumbnailsDict.get('mat').get(15).get(22), 'gf_mat_15_22.jpg', "Thumbnail file name from dict should be [%s]" % 'gf_mat_15_22.jpg')
        self.assertEqual(thumbnailsDict.get('luk').get(15).get(3), 'gf_luk_15_03.jpg', "Thumbnail file name from dict should be [%s]" % 'gf_luk_15_03.jpg')
        self.assertEqual(thumbnailsDict.get('jhn').get(3).get(2), 'gf_jhn_03_02.jpg', "Thumbnail file name from dict should be [%s]" % 'gf_jhn_03_02.jpg')
        self.assertEqual(thumbnailsDict.get('jhn').get(22), None, "Thumbnail file name from dict should be [%s]" % 'None')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(thumbnailsDict, "mat", 15, 17), 'gf_mat_15_22.jpg', "Thumbnail file name should be [%s]" % 'gf_mat_15_22.jpg')
        self.assertEqual(self.updateService.getThumbnailNameFromDict({}, "jhn", 1, 1), None, "Thumbnail file name should be [%s]" % 'None')
        self.assertEqual(self.updateService.getThumbnailNameFromDict(None, "luk", 1, 1), None, "Thumbnail file name should be [%s]" % 'None')

if __name__ == '__main__':
    unittest.main(argv=['test'], exit=False)
