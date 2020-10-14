#!/usr/bin/perl

##### ARGUMENTS can be one or more [dir/]bible_id/fileset_id, or -g \d{1}, or -s
$group = 4;
$sync = 0;
while ($arg = shift @ARGV) {
    if ($arg=~/\//) {
        push @filesets, $arg;
    }
    elsif ($arg=~/^-g$/) {
        $group = shift @ARGV;
        if (not $group=~/^\d$/) {
            printf STDERR "ERROR: a single digit must follow -g argument (given value was $group)\n" and die;
        }
    }
    elsif ($arg=~/^-s$/) {
        $sync=1;
    }
    else {
        print STDERR "$arg: FAILED - unrecognized format\n";
    }
}

##### INITIALIZE
$|=1; # don't buffer
use Digest::MD5 qw(md5_hex);
use File::Basename;
use File::Path qw(make_path);
# open database connection
use DBI;
use DBD::mysql;
$dsn = "DBI:mysql:database=dbp_newdata;host=127.0.0.1;port=3310;mysql_client_found_rows=TRUE";
my $dbh = DBI->connect($dsn,"sa","pS193DNhaGxoO5v") or die; 

##### WORK
foreach $fileset (@filesets) {
    add_fileset($fileset)
}

##### SUBROUTINES
# make an string appropriate for use as field list in extended INSERT statements
sub fldstr {
    return "(".join(",",@_).")";
}
# make an string appropriate for use as values in extended INSERT statements
sub valstr {
    return "('".join("','",@_)."')";
}
# main entry
sub add_fileset() {
    my $start = time;
    my $dir = shift @_;
    if ($dir=~/([A-Z0-9]+)\/([A-Z0-9]{7}[12]DV)/) {
        $bible_id = $1;
        $fileset_id = $2;
    }
    else {
        print STDERR "FAILED: improperly formatted $dir\n" and goto RETURN;
    }
    print "$bible_id/$fileset_id: ";

    if (-d $dir and not -w $dir) {
        print STDERR "FAILED: can't write directory $dir\n";
        return;
    }
    elsif (not -d $dir and not make_path($dir)) {
        print STDERR "FAILED: can't make directory $dir: $!\n";
        return;
    }

    # sync m3u8 files from s3 if none found (to re-sync, simply remove before running)
    $m3u8 = join /" "/, glob "$dir/*.m3u8";
    if ($sync or not ($m3u8 =~ /_stream\.m3u8/ and $m3u8 =~ /0p\.m3u8/)) {
        $cmd = "aws --profile dbs s3 sync s3://dbp-vid/video/$bible_id/$fileset_id/ $dir/ --exclude \"*\" --include \"*.m3u8\"";
        @cmdout = `$cmd`;
        $m3u8 = join /" "/, glob "$dir/*.m3u8";
        unless ($m3u8 =~ /_stream\.m3u8/ and $m3u8 =~ /0p\.m3u8/) {
            print STDERR "don't see *_stream.m3u8 and *0p.m3u8 files resulting from `$cmd`\n", @cmdout and goto RETURN;
        }
    }
    print "m3u8, ";

    $hash_id = substr(md5_hex($fileset_id."dbp-vid"."video_stream"), 0, 12);
    $prod_id = substr(md5_hex($fileset_id."dbp-prod"."video_stream"), 0, 12);
    # clean slate
    my %streams = ();
    my %streamfiles = ();
    my @values = ();

#####
$out="$dir/bible_files.csv";
@streams = glob "$dir/*_stream.m3u8";
# lookup for sorting by book
%booknum = qw(MAT 01 MRK 02 LUK 03 JHN 04);
foreach $stream (@streams) {
    $stream = basename($stream);
    if (not $stream=~/End_Credits/) {
	# verse start/end may have a or b (if verse break inside verse), or r (for revision)
        ($book, $chap, $vstart, $vend) = $stream=~/[^_]+_([A-Z]+)_(\d+)-(\d+)[abr]*-(\d+)[abr]*_/;
        unless (exists $booknum{$book}) {
	        print STDERR "Problem parsing book from: $stream\n" and goto RETURN;
        }
	    $c = (length $chap   == 1) ? "0$chap"   : $chap;
	    $v = (length $vstart == 1) ? "0$vstart" : $vstart;
        # key constructed to facilitate sorting in next section
	    $streams{$booknum{$book}.$c.$v} = valstr($hash_id,$book,$chap,$vstart,$vend,$stream);
	    $streamfiles{$booknum{$book}.$c.$v} = "$stream";
    }
	else {
		($book) = $stream=~/_([A-Z]+)_End_Credits/;
	    $streams{$booknum{$book}."CREDITS"} = valstr($hash_id,$book,"CHAP","VERS","NULL",$stream);
	    $streamfiles{$booknum{$book}."CREDITS"} = "$stream";
    }
}
open OUT, ">$out" or die "can't open $out: $!";
foreach $stream (sort keys %streams) {
    if ($stream=~/CREDITS/) {
        ($c,$v) = $streams{$prev}=~/(\d+)','\d+','(\d+)',/;
	    $v = $v + 1;
        $streams{$stream} =~ s/CHAP/$c/;
        $streams{$stream} =~ s/VERS/$v/;
        $streams{$stream} =~ s/NULL/$v/;
    }
    print OUT "$streams{$stream}\n";
    push @values, $streams{$stream};
    $prev = $stream;
}
close OUT;

# open sql log file
$sqllog="$dir/$fileset_id.sql";
open SQLLOG, ">$sqllog" or die "can't open $sqllog: $!";

##### filesets
# delete corresponding dbp-prod (if exists)
$qry = "DELETE FROM bible_filesets WHERE hash_id='$prod_id';";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;

# try to update updated_at value
$qry = "UPDATE bible_filesets SET updated_at=CURRENT_TIMESTAMP WHERE hash_id='$hash_id';";
print SQLLOG "$qry\n";
$updated = $dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
if ($updated==1) { # a row was updated, so record exists, so just prune related records (and CASCADEs)
    $qry = "DELETE FROM bible_files WHERE hash_id='$hash_id';";
    print SQLLOG "$qry\n";
    $dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
    # we'll reset access_group, so delete any existing value(s)
    $qry = "DELETE FROM access_group_filesets WHERE hash_id='$hash_id';";
    print SQLLOG "$qry\n";
    $dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
    # also delete connection, so we don't risk connection to half-baked fileset (we'll re-create it last...)
    $qry = "DELETE FROM bible_fileset_connections WHERE hash_id='$hash_id';";
    print SQLLOG "$qry\n";
    $dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
}
else { # a row was not found, so create it
    $fld = '(id,hash_id,asset_id,set_type_code,set_size_code)';
    $val = valstr($fileset_id,$hash_id,"dbp-vid","video_stream","NTP");
    $qry = "INSERT INTO bible_filesets $fld VALUES $val;";
    print SQLLOG "$qry\n";
    $dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
}
print "filesets, ";

##### files
$fld = '(hash_id,book_id,chapter_start,verse_start,verse_end,file_name)';
$val = join(",", @values);
$qry = "INSERT INTO bible_files $fld VALUES $val;";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: running `$qry`:\n$DBI::errstr\n" and goto RETURN;
# note: duration is empty because we don't know it yet, but we'll calculate it later...
print "files, ";

##### resolutions
$out="$dir/bible_file_video_resolutions.csv";
open OUT, ">$out" or die "can't open $out: $!";
$qry = "SELECT id, file_name FROM bible_files WHERE hash_id='$hash_id' ORDER BY id;";
print SQLLOG "$qry\n";
my $sth = $dbh->prepare($qry);
$sth->execute();
@values = ();
while (my $ref = $sth->fetchrow_hashref()) {
    $stream = $ref->{'file_name'};
    open S, "$dir/$stream" or die "can't open $dir/$stream: $!";
    while (<S>) {
	if (/STREAM/) {
            ($b,$w,$h,$c)=/BANDWIDTH=(\d+),RESOLUTION=(\d+)x(\d+),CODECS="(.+)"/;
	}
	elsif (/m3u8/) {
	    chomp;
            print OUT "$ref->{'id'},$stream,$b,$w,$h,$c,1\n";
	    push @values, valstr($ref->{'id'},$_,$b,$w,$h,$c,1);
            push @resfiles, $_;
	}
    }
    close S;
}
close OUT;
$sth->finish();
$fld = '(bible_file_id,file_name,bandwidth,resolution_width,resolution_height,codec,stream)';
$val = join(",", @values);
$qry = "INSERT INTO bible_file_video_resolutions $fld VALUES $val;";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: $DBI::errstr\n" and goto RETURN;
print "resolutions, ";

##### streams
$qry = "SELECT r.id AS id, r.file_name AS file_name FROM bible_file_video_resolutions r JOIN bible_files f ON r.bible_file_id=f.id WHERE f.hash_id='$hash_id' ORDER BY r.id;";
print SQLLOG "$qry\n";
my $sth = $dbh->prepare($qry);
$sth->execute();
@values = ();
$out="$dir/bible_file_video_transport_stream.csv";
open OUT, ">$out" or die "can't open $out: $!";
while (my $ref = $sth->fetchrow_hashref()) {
    $stream = $ref->{'file_name'};
    open S, "$dir/$stream" or die "can't open $stream: $!";
    while (<S>) {
	    if (/EXTINF:([0-9.]+),/) {
            $dur=$1;
	    }
	    elsif (/\.ts$/) {
	        chomp;
            print OUT "ID,$_,$dur\n";
	        push @values, valstr($ref->{'id'},$_,$dur);
	    }
    }
    close S;
}
# mysql default max_allowed_packet is 4MB.  $qry above is 800K for MRK with 3 resolutions,
# so no attempt made for now to break into smaller chunks.  error should be clear if encountered.
$sth->finish();
$fld = '(video_resolution_id,file_name,runtime)';
$val = join(",", @values);
$qry = "INSERT INTO bible_file_video_transport_stream $fld VALUES $val;";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: $DBI::errstr\n" and goto RETURN;
print "streams, ";

##### durations
$qry = "UPDATE bible_files bf INNER JOIN
(SELECT DISTINCT bf.id AS ID, SUM(bfvts.runtime) AS Duration
FROM bible_files bf
JOIN bible_filesets bfs ON bfs.hash_id=bf.hash_id
JOIN bible_file_video_resolutions bfvr ON bfvr.bible_file_id=bf.id
JOIN bible_file_video_transport_stream bfvts ON bfvts.video_resolution_id=bfvr.id
WHERE bf.id IN (
SELECT bf.id
FROM bible_files bf
WHERE bf.hash_id='$hash_id'
AND bf.duration IS NULL)
GROUP BY bf.id, bfvr.id) bfu
ON bf.id=bfu.ID
SET bf.duration=bfu.Duration;";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: $DBI::errstr\n" and goto RETURN;
print "durations, ";

# TODO: add the below steps
##### copyrights organizations
# associate org id = 541 and org role = 1
##### bible_filesets_copyrights
# append the below onto the end of the corresponding DA copyright
# Video: Courtesy of LUMO Project Filmes

##### permission
$fld = '(access_group_id, hash_id)';
$val = "('$group','$hash_id')";
$qry = "INSERT INTO access_group_filesets $fld VALUES $val;";
print SQLLOG "$qry\n"; 
$dbh->do($qry) or print STDERR "FAILED: $DBI::errstr\n" and goto RETURN;
print "access, ";

##### connections
$fld = '(hash_id, bible_id)';
$val = "('$hash_id','$bible_id')";
$qry = "INSERT INTO bible_fileset_connections $fld VALUES $val;";
print SQLLOG "$qry\n";
$dbh->do($qry) or print STDERR "FAILED: $DBI::errstr\n" and goto RETURN;
print "connections - DONE!\n";

RETURN:
close OUT;
close SQLLOG;
printf "  (%d seconds)\n", time - $start;
return;
}
