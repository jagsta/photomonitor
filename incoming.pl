#!/usr/bin/perl -w
#
# the premise is that this script will receive an intotifywaut stream via
# STDIN thus:
# inotifywait -q -m -r --exclude '[.]jpg$' -e close_write -e create -e delete /media/photos/ | perl photomonitor.pl 
# This will monitor for new photos or pgotos being deleted, and convert raws to jpeg web format pics, or from high res jpg to web format, and copy these to the web serving directory for zenphoto, which will also be used as the root directory for picasa, lightroom etc (picasa will dynamically reflect additons, lightroom will not)
#
# THINGS TO DO:
# 1. Make Image logic into sub
# 2. Make video logic sub
# 3. Create child code near top of script - minimise variables and unidirectional pipe
# 4. Add MOVE_TO code
# 5. invoke a grandchild with bidirectional pipe
# 5. On event via STDIN, process event, on file ready for processing pass file to child
# 6. Child adds from STDIN to FIFO, it then checks to see if there is a file being processed, (internal flag) and then if not sends next from FIFO to grandchild, if file being processed continue looping
# 7. Grandchild waits for STDIN (read pipe from parent), when receives it processes, any errors cause fail <file> error type response to parent via write pipe
# 8. If successful grandchild sends an ok <file> to the parent
# 9. Parents receives ok or fail then clears processing flag
# 10. All 3 processes persist
#
# Parent - needs pipe for child, syslog, debug
# Child - needs watched hash, processing flag, syslog, debug
# Grandchild - needs dirhash, month hash, videostub, jpegstub, archivestub
#
use strict;
use Fcntl;
use English '-no_match_vars';
use File::Copy;
use Sys::Syslog qw( :DEFAULT setlogsock);
use IO::Handle;
use IO::Select;
use Socket;
use Digest::MD5;
use Linux::Inotify2;
#use File::Touch;

# Allow yhr OS to reap children which are orphaned. We shouldn't need this but it's a safety net.
$SIG{CHLD} = 'IGNORE';
$SIG{INT} = \&ctrlc;
$SIG{TERM} = \&term;

# Setup syslog 
setlogsock('unix');
openlog($0,'','user');

$0 =~ m/(.+\/)*(.+?)$/;
my $programname=$2;

# Check for lockfile existence and deal appropriately
my $lockfile="/tmp/" . $2 . ".lock";
if (-e $lockfile){
	syslog('info',"$2 Lock file detected, exiting");
	exit;
}
else{
	syslog('info', "$0 started");
	`touch "$lockfile"`;
};

# Some global variables (I need to wort out passing hashes to subroutines!	
my $debug = "1"; 				# Debug level
my %month = (	'Jan' => '01',	# hash of month to digits
		'Feb' => '02',
		'Mar' => '03',
		'Apr' => '04',
		'May' => '05',
		'Jun' => '06',
		'Jul' => '07',
		'Aug' => '08',
		'Sep' => '09',
		'Oct' => '10',
		'Nov' => '11',
		'Dec' => '12' );
my %dirhash;					# hash of checked directories
my $kidpid;						# Spooler PID
my $sleep_count;				# Atemnpts to spawn
do {
	$kidpid = open(SPOOLER, "|-");
	unless (defined $kidpid) {
		syslog('alert', "$$ WATCHER Cannot fork: $!");
		if ($sleep_count++ > 6){
			syslog('alert', "$$ WATCHER Giving up after 6 attempts to fork");
			die;
		};
		sleep 5;
	} 
} until defined $kidpid;
if ($kidpid){ 					# This is the parent code
	syslog('info',"$$ WATCHER forked SPOOLER process $kidpid");
	# Flush SPOOLER instead of buffering
	select((select(SPOOLER), $|=1)[0]);
	my %watchedhash;
	my %inotifywatches;
	my $sourcepath;
	my $e;
	my @events;
	my $event;
	my $name;
	my $eventtype;
	my $inotify = new Linux::Inotify2 or die "Unable to create inotify object";
	if ( -d "/storage/media/incoming")
	{
		$inotify->watch("/storage/media/incoming", IN_MODIFY | IN_MOVED_TO | IN_CLOSE_WRITE | IN_CREATE) or die "$!";
	} 
	else 
	{
		die "Watched dir doesn't exist"
	}
	while (1) {			# This is the busiest thread by far, minimise variable assignments etc.
		@events=$inotify->read;
		foreach $event (@events){
			$name=$event->fullname;
			if ($event->IN_ISDIR && $event->IN_CREATE){
				if ($debug>1) {syslog('debug',"$$ WATCHER adding watch to $name");}
				$inotifywatches{$name}=$inotify->watch($name, IN_MODIFY | IN_MOVED_TO | IN_CLOSE_WRITE | IN_CREATE | IN_DELETE_SELF) or die "$!";
			}
			elsif ($event->IN_MODIFY){
				unless (exists $watchedhash{$name}){
					if ($debug>1) {syslog('debug',"$$ WATCHER matched MODIFY action $name");}
					$watchedhash{$name}{modify}=1;
				};
			}
			elsif ($event->IN_CLOSE_WRITE){
				if (exists $watchedhash{$name}){
					print SPOOLER "$name\n";
					if ($debug>1) {syslog('info',"$$ WATCHER passing $name to SPOOLER process $kidpid after CLOSE_WRITE");}
					delete $watchedhash{$name};
				}
			}
			elsif ($event->IN_MOVED_TO){
				print SPOOLER "$name\n";
				if ($debug>1) {syslog('info',"$$ WATCHER passing $name to SPOOLER process $kidpid after MOVED_TO");}
			}
			elsif ($event->IN_DELETE_SELF){
				if ($debug>1) {syslog('debug',"$$ WATCHER removing watch for $name");}
				$inotifywatches{$name}->cancel;
				delete $inotifywatches{$name};
			}
			elsif ($event->IN_Q_OVERFLOW){
				syslog('info',"$$ WATCHER missed events for $name due to queue overflow");
			}
		}
	};

# SPOOLER
} 
else { # This is the child
	my $grandkidpid;
	my $maxworkers=2;
	my $numworkers=0;	
	my $childpipe;
	my $parentpipe;
	my $selecthandle = IO::Select->new();
	$selecthandle->add(\*STDIN);
	while ($numworkers < $maxworkers) {
		$numworkers++;	
		socketpair($childpipe, $parentpipe, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "socketpair: $!";
		$childpipe->autoflush(1);
		$parentpipe->autoflush(1);
		if ($grandkidpid = fork) { # This is still the child
			syslog('info',"$$ SPOOLER forked child PROCESSOR thread $grandkidpid");
			close $parentpipe;
			$selecthandle->add($childpipe);
			undef $childpipe;
			undef $parentpipe;
		}
		else { # This is the PROCESSOR 
			die "cannot fork: $!" unless defined $grandkidpid;
			close $childpipe;
			my $videostub="/storage/media/video/home.videos";
			my $jpegstub="/storage/media/photoweb";
			my $archivestub="/storage/media/photos";
			my $childhandle = IO::Select->new();
			my @fhs;
			$childhandle->add($parentpipe);
			while (1) {
				if (@fhs = $childhandle->can_read(10)){
					foreach my $fh (@fhs){
						my $message = <$fh>;	
						if (defined $message){chomp $message}else{$message=""};
						# All the magic goes here
						if ($message=~m/(.+)$/){
							my $response=$1;
							if ($debug>1) {syslog ('debug',"$$ PROCESSOR$numworkers has received $response from SPOOLER");}
							if ($debug>2) {syslog ('debug',"$$ PROCESSOR$numworkers is subbing $response $jpegstub $archivestub");}	
							my $result=&process_image($response,$jpegstub,$archivestub,$numworkers);
							if ($result=~m/ignore/){
								if ($debug>1) {syslog ('info',"$$ PROCESSOR$numworkers is sending ignore: $result to SPOOLER");}
								print $parentpipe "ignore: $response $result\n";
							}
							elsif ($result){
								if ($debug>1) {syslog ('info',"$$ PROCESSOR$numworkers is sending fail: $result to SPOOLER");}
								print $parentpipe "fail: $response $result\n";
								syslog ('alert',"$$ PROCESSOR$numworkers image processing failed for $response $result");
							}
							else {
								if ($debug>1) {syslog ('info',"$$ PROCESSOR$numworkers is sending ok: $response to SPOOLER");}
								print $parentpipe "ok: $response\n";
							};
						}
					}
				}
				else {
					print $parentpipe "ready\n";
				};
			};
		exit;
		};
	};
	# SPOOLER
	my @fifo;
	my @fhs;
	#my $supported_types='(nef|jpg|jpeg|crw|avi|mts|m2ts|mkv|mov|mpg|mpeg)$'
	my $supported_types='(nef|jpg|jpeg|crw|cr2)$';
	my $excluded_filenames='(picasa.ini|AppleDouble|DS_Store|\.[\./w/s/d-]*_)';
	my $processed=0;
	my $failed=0;
	my $ignored=0;
	my $received=0;
	my $unsupported=0;
	my $processing=0;
	my $fifosize=0;
    while (1){
		if (@fhs = $selecthandle->can_read){
			foreach my $fh (@fhs){
				if ($fh == \*STDIN){
					chomp (my $message = <STDIN>);
					#$message=~m/(.+)$/;
					if ($debug>1) {syslog ('info',"$$ SPOOLER has received $message from WATCHER");}
					if (($message=~m/$supported_types/i) && ($message!~m/$excluded_filenames/)){
						$fifosize= (push @fifo, $message);
						$received++;
						if ($debug>1)	{syslog ('debug',"$$ SPOOLER has now received $received files to process");}
					}	
					else {
						syslog ('info',"$$ SPOOLER ignoring $message as unsupported filetype");
						$unsupported++;
					};
				}
   	         	#if ($fh == \*CHILD){
   	         	else {
					my $message = <$fh>;		
					if (defined $message){chomp $message}else{$message=""};
					if (($message=~m/^ready$/) && ($fifosize>0)){
						#send first job to this child
						my $message = shift @fifo;
						$fifosize=@fifo;
						print $fh "$message\n";
						if ($debug>1){syslog('debug',"$$ SPOOLER is sending $message to PROCESSOR");}
						$processing++;
					}
					elsif ($message=~m/^(\w{2,6}): (.+)$/)
					{
						if ($debug>1) {syslog('debug',"$$ SPOOLER has received $message from PROCESSOR");}
						$processing--;
						if ($1 eq "ok"){
							# File was processed successfully
							$processed++;
							syslog ('info',"$$ SPOOLER image archiving complete for $2");
						}
						elsif ($1 eq "fail") {
							#file archival failed
							$failed++;
						}
						elsif ($1 eq "ignore"){
							$ignored++;
						}
						if (($fifosize > 0) && ($processing < $maxworkers)){
							my $message = shift @fifo;
							$fifosize=@fifo;
							print $fh "$message\n";
							if ($debug>1){syslog('debug',"$$ SPOOLER is sending $message to PROCESSOR");}
							$processing++;
						}
						if ($debug) {syslog('info',"$$ SPOOLER queue:$fifosize processed:$processed failed:$failed ignored:$ignored unsupported:$unsupported");}
					}
				}
			}
		}
	}
	exit;
} 

closelog;

sub check_directory
{
	my ($dir,$workerid) = @_;
	if ($debug>1) {syslog('debug',"$$ PROCESSOR$workerid checking for destination directory $dir");}
	unless (exists $dirhash{$dir}){
		unless (-e $dir) {
			unless (mkdir $dir){
				syslog('alert',"$$ PROCESSOR$workerid failed directory creation! $!");
				return ("Failed Directory Creation $dir");
			} 
			else {
				$dirhash{$dir}=1;
			}
		}
		else {
			if ($debug>1) {syslog('debug',"$$ PROCESSOR$workerid dir $dir exists, adding to cache");}
			$dirhash{$dir}=1;
		}
	}
	else {
		if ($debug>1) {syslog('debug',"$$ PROCESSOR$workerid hit dir cache for existing directory");}
	}
	return 0;
};

sub process_image 
{
	my ($sourcepath,$jpegstub,$archivestub,$workerid)=@_;
	$sourcepath=~m/^.+\/(.+?)$/;
	my $filename=$1;
	# Firstly work out where we're going to put this picture
	#my $date=`/usr/bin/dcraw -i -v "$sourcepath" | grep Timestamp 2>&1`;
	my $date=`/usr/bin/exiftool "$sourcepath" | grep Modification 2>&1`;
	if ($date=~m/Time\s+:\s+(\d+):(\d+):(\d+)\s+(.+)$/){
		#my $day = $3;
		#if (length ($day) == 1) {$day = "0" . $day};
		my $dirname = $1 . "-" .  $2 . "-" . $3;
		$date = $1 . "-" . $2 . "-" . $3 . " " . $4;
		my $archivedir = $archivestub . "/" . $1 . "/";
		my $jpegdir = $jpegstub . "/" . $1 . "/";
		my $result = &check_directory($archivedir,$workerid);
		if ($result){
			syslog('info',"$$ PROCESSOR$workerid warning: $result");
		}
		$result = &check_directory($jpegdir,$workerid);
		if ($result){
			syslog('info',"$$ PROCESSOR$workerid warning: $result");
		}
		$archivedir = $archivedir .  $dirname . "/";
		$jpegdir = $jpegdir . $dirname . "/";
		$result = &check_directory($archivedir,$workerid);
		if ($result){
			syslog('info',"$$ PROCESSOR$workerid warning: $result");
		}
		$result = &check_directory($jpegdir,$workerid);
		if ($result){
			syslog('info',"$$ PROCESSOR$workerid warning: $result");
		}
		my $archivepath = $archivedir . $filename;	
		if ($debug>2) {syslog('debug', "$$ PROCESSOR$workerid archive file is now $archivepath");}
		my $sourcemd5;
		my $archivemd5;
		if (-e $archivepath){
			if ($debug>2) {syslog('debug',"$$ PROCESSOR$workerid $archivepath exists, comparing md5sum with $sourcepath");}
			$sourcemd5=&checksum($sourcepath);
			$archivemd5=&checksum($archivepath);
			if ($sourcemd5 eq $archivemd5){
				if ($debug>2){syslog('debug',"$$ PROCESSOR$workerid md5sums $sourcemd5 and $archivemd5 are identical");}
				if ($debug) {syslog('info',"$$ PROCESSOR$workerid file $archivepath exists, ignoring");}
				#sleep 1;
				unlink $sourcepath;
				return ("ignore $sourcepath");
			}
			else {
				# the files are not identical, rename tnen copy as normal
				syslog('info',"$$ PROCESSOR$workerid duplicate filename $archivepath but different checksum $sourcemd5 and $archivemd5");
				($result,$filename)=&rename($archivedir,$filename,$workerid,$sourcemd5);
				return ("Failed to rename $archivepath") if ($result);
			}
		}
		$archivepath = $archivedir . $filename;
		my $jpegpath = $jpegdir . $filename;
		$jpegpath =~s/(nef|crw|cr2|jpeg)$/jpg/i;
		($result)=&replicate($sourcepath,$archivepath,$jpegpath,$date,$workerid);
		return ("Failed to replicate $sourcepath") if ($result);
		return 0;
	}
	return ("Cannot get Timestamp: $date");
};


sub rename
{
	my ($fullstub,$filename,$workerid,$sourcemd5)=@_;
	if ($debug) {syslog('info',"$$ PROCESSOR$workerid trying to rename $fullstub$filename");}
	my $version=1;
	my $newmd5;
	$filename=~m/(.+)\.(.+?)$/;
	$filename = $1 . "_" . $version . "." . $2;
	my $fullpath= $fullstub . $filename;
	if (-e $fullpath) {$newmd5 = &checksum($fullpath);}else{$newmd5=""};
	while ((-e $fullpath) && ($newmd5 ne $sourcemd5 )) {
		$version++;
		$filename = $1 . "_" . $version . "." . $2;
		$fullpath = $fullstub . $filename;
		if (-e $fullpath) {$newmd5 = &checksum($fullpath);}else{$newmd5=""};
		if ($debug>1) {syslog ('debug',"$$ PROCESSOR$workerid, renaming archive copy, trying $fullpath");}
		return (1,$fullpath) if ($version>10);
	}
	syslog('info',"$$ PROCESSOR$workerid renamed archive copy to $fullpath");
	return (0,$filename);
};

sub replicate
{
	my ($sourcepath,$archivepath,$jpegpath,$date,$workerid)=@_;
	if ($debug) {syslog('info',"$$ PROCESSOR$workerid copying original file $sourcepath to $archivepath");}
	unless (copy($sourcepath,$archivepath)) {
		syslog('alert',"$$ PROCESSOR$workerid failed to copy $sourcepath to $archivepath! $!");
		return ("Failed copy operation to $archivepath");
	}
	else {
		my $sourcemd5=&checksum($sourcepath);
		my $archivemd5=&checksum($archivepath);
		unless ($sourcemd5 eq $archivemd5) {
			syslog('alert',"$$ PROCESSOR$workerid crc comparison failure for $sourcepath and $archivepath");
			return ("Failed checksum");
			unlink $archivepath;
		}
		else {
			# Added 18/12/2012 to chown to users group
			
			if ($debug) {syslog('info',"$$ PROCESSOR$workerid retouching original $archivepath to date taken $date");}
			if ($date) {`touch -d "$date" "$archivepath" 2>&1`;}
			if ($sourcepath=~m/\.(nef|crw|cr2)$/i){
				if ($debug) {syslog('info',"$$ PROCESSOR$workerid executing dcraw-thumb with parameters $sourcepath $jpegpath");}
				my $result = `nice -n 10 /usr/bin/dcraw -e -c -w "$sourcepath" > "$jpegpath" 2>&1`;
				unless ($result){
					`touch -d "$date" "$jpegpath"`;
				}
				else {return ("Failed archive operation to $jpegpath");}
			}	
			elsif ($sourcepath=~m/\.(jpg|jpeg)$/i) {
				if ($debug) {syslog('info',"$$ PROCESSOR$workerid Executing jpeg-thumb with parameters $sourcepath $jpegpath");}
				my $result = `nice -n 10 /usr/bin/convert -quality 90 "$sourcepath" "$jpegpath" 2>&1`;
				unless ($result){
					`touch -d "$date" "$jpegpath"`;
				}
				else {return ("Failed mirror operation to $jpegpath");}
			}	
		unlink $sourcepath;
		}
	}
	return (0);
};

sub checksum
{
	my $file=shift;
    open (FILE, "<$file");
   	binmode(FILE);
    my $result = Digest::MD5->new->addfile(*FILE)->hexdigest;
	close FILE;
	return $result;
        #
	#my $result= `cksum "$file"`;
	#syslog('debug',"checksumming $file with result $result");
	#if ($result=~m/^(\d+)\s+.+$/){
	#	return $1;
	#}
	#else {
#		return $file;
#	}
};

sub ctrlc {
	$SIG{INT} = \&ctrlc;
	syslog('alert',"$$ interrupt received, exiting");
	unlink $lockfile;
	exit;
}

sub term {
	$SIG{TERM} = \&term;
	syslog('alert',"$$ kill received, exiting");
	unlink $lockfile;
	exit;
}
