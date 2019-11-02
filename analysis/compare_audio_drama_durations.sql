




CREATE TEMPORARY TABLE audio_durations AS
(SELECT concat(left(bs.id,7), mid(bs.id,9,14)) AS id, bf.book_id, bf.chapter_start, bft.value
FROM bible_filesets bs, bible_files bf, bible_file_tags bft
WHERE bs.hash_id = bf.hash_id
AND bf.id = bft.file_id
AND bs.set_type_code='audio'
AND bft.tag = 'duration'
AND bf.chapter_start is NOT NULL
AND bs.id not like 'SNMNVS%');

CREATE UNIQUE INDEX audio_idx ON audio_durations (id, book_id, chapter_start);

CREATE TEMPORARY TABLE drama_durations AS
(SELECT concat(left(bs.id,7), mid(bs.id,9,14)) AS id, bf.book_id, bf.chapter_start, bft.value
FROM bible_filesets bs, bible_files bf, bible_file_tags bft
WHERE bs.hash_id = bf.hash_id
AND bf.id = bft.file_id
AND bs.set_type_code='audio_drama'
AND bft.tag = 'duration'
AND bf.chapter_start is NOT NULL
AND bs.id not like 'SNMNVS%');

CREATE UNIQUE INDEX drama_idx ON drama_durations (id, book_id, chapter_start);

SELECT t1.id, t1.book_id, t1.chapter_start, t1.value, t2.value
FROM audio_durations t1, drama_durations t2
WHERE t1.id = t2.id
AND t1.book_id = t2.book_id
AND t1.chapter_start = t2.chapter_start
AND t1.value != t2.value

===== try another approach that uses a sum of duration for the fileset ====

CREATE TEMPORARY TABLE audio_durations AS
(SELECT concat(left(bs.id,7), mid(bs.id,9,14)) AS id, sum(bft.value) AS duration
FROM bible_filesets bs, bible_files bf, bible_file_tags bft
WHERE bs.hash_id = bf.hash_id
AND bf.id = bft.file_id
AND bs.set_type_code='audio'
AND bft.tag = 'duration'
GROUP BY concat(left(bs.id,7), mid(bs.id,9,14)));

CREATE UNIQUE INDEX audio_idx ON audio_durations (id);

CREATE TEMPORARY TABLE drama_durations AS
(SELECT concat(left(bs.id,7), mid(bs.id,9,14)) AS id, sum(bft.value) AS duration
FROM bible_filesets bs, bible_files bf, bible_file_tags bft
WHERE bs.hash_id = bf.hash_id
AND bf.id = bft.file_id
AND bs.set_type_code='audio_drama'
AND bft.tag = 'duration'
GROUP BY concat(left(bs.id,7), mid(bs.id,9,14)));

CREATE UNIQUE INDEX drama_idx ON drama_durations (id);

select t1.id, t1.duration, t2.duration, abs(t1.duration - t2.duration)
from audio_durations t1, drama_durations t2
where t1.id=t2.id
and t1.duration != t2.duration

