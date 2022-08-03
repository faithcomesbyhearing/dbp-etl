  # dbp-etl 
  
  This code is used to push verse timings into DB. There are 2 steps. 
  
  1) Load timings data into the bible_file_timestamps table
  Sample run: python3 py/BibleFileTimestamps_Insert_aeneas_NT.py -aeneas_timing_dir path_to_verse_timings_dir/ENGWEBN2DA -aeneas_timing_err 4
  The code gets the filesetid from the path , in this case its ENGWEBN2DA. So it knows under what filesetid to push the timings to. 
  aeneas_timing_err is to store the process in which timings were computed, in this case its using aeneas
  Sample Quality codes:
  0 = verified by known native speaker
  1 = verified by unknown native speaker
  2 = verified by non-native speaker
  3 = verified by non-speaker (eg members of the text team at FCBH who do this type of thing for a living)
  4 = automated aeneas timings quality controlled by qinfo line timings
  5 = automated spot-checked by non-native speaker (this is the case of most of the SAB appDef timings)
  9 = automated unverified (probably would never put these into the DB, but if I did at least I’d have this indicator of non-confidence)
  
  Under the path_to_verse_timings_dir/ENGWEBN2DA, we have 260 timing files for each chapter in the NT. Example below:
  C01-01-MAT-01-timing.txt
  7.17	16.02	1
  16.02	52.82	6
  52.82	92.53	11
  92.53	128.14	16
  128.13	150.85	17
  150.85	165.25	18
  165.25	176.27	19
  176.27	198.18	20
  198.19	208.73	21
  208.73	216.62	22
  216.62	229.58	23
  229.58	241.63	24
  241.63	253.3	25
  
  For OT, 
  python3 py/BibleFileTimestamps_Insert_aeneas_OT.py -aeneas_timing_dir path_to_verse_timings_dir/ENGWEBO2DA -aeneas_timing_err 4
  The code can be combined into one under BibleFileTimestamps_Insert_aeneas.py, but I differentiated it initially to accommodate the difference in the NT and OT (# chapters , naming conventions etc. ) 
  
  2) Create corresponding HLS filesets in DB (no extra files in S3 are needed) for given filesetid
  Sample run: python3 py/AudioHLS.py newdata SPNWTCN2DA
  
  The following insert operations are done.
        INSERT INTO bible_filesets (eg ENGESVN2SA)
        stream_hashid = SELECT FROM bible_filesets WHERE id = ENGESVN2SA
        local_mp3s = get list of local mp3's (download from s3 if needed)
             enforce cannon book/chapter order in mp3s or sql query, and foreach on that (for progress monitoring)
        foreach chapter   SELECT file_name FROM bible_files WHERE hash_id=hash_id
          if new book, print first letter of book (or . for missing books in cannon-order sequence)
          foreach bitrate_hashid   DISTINCT(description) FROM bible_fileset_tags WHERE hash_id=hashid AND name='bitrate'
              do work
            verify chapter is available in mp3s (eg B01___01_Matthew_____ENGESVN2DA.mp3)
            use ffprobe to determine bitrate
            [bytes,offsets,durations] = use ffprobe to determine verse offset, bytes, and duration
              save work
            INSERT INTO bible_files (stream_hashid, eg B01___01_Matthew_____ENGESVN2DA.m3u8)
            fileid = SELECT FROM bible_files (what we inserted above)
            INSERT INTO bible_files_stream_bandwidths (fileid, eg B01___01_Matthew_____ENGESVN2DA-bitrate.m3u8, bitrate)
            bwid = SELECT FROM bible_files_stream_bandwidths (what we inserted above)
            INSERT INTO bible_files_stream_segments (bwid, timestamps.id, [bytes,offsets,durations])
  
  
  
