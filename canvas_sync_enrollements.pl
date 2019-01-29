#!/usr/bin/perl

## Using the list of canvas_courses.txt select from the CM tables and import into canvas using SIS import
## 2019-01-29: Corne Oosthuizen

use strict;
use utf8;
binmode STDOUT, ':utf8';

use DBI;
use HTTP::Request::Common;
use LWP;
use JSON::XS;
use Data::Dump 'dump';
use Date::Manip;
use POSIX qw(strftime);
use Time::Duration;
use Getopt::Long;
use List::Util qw/uniq all/;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use File::Copy;
use File::Path qw(make_path);

require '/usr/local/sakaiconfig/vula_auth.pl';

my $production = 0;
my $help = 0;

GetOptions ('prod' => \$production, 'help' => \$help)
  or die("Error in command line arguments\n");

if ($help) {
    print <<'HELPTEXT';

Options:
  --prod (Run with production settings)
  --help (This view)

HELPTEXT
    exit(1);
}

my ($dbname, $dbhost, $username, $password ) = getDbConfig();
my ($host, $auth_token) = $production ? getCanvas() : getCanvasTest();

my $uri = "https://". $host ."/api/v1/accounts/self/sis_imports/";

my $script_folder = "/usr/local/canvas-scripts/";

my $script_folder_tmp = $script_folder ."tmp/";
my $script_folder_done = $script_folder ."done/";

unless(-e $script_folder_tmp){ make_path($script_folder_tmp) or die "Failed to create path: $script_folder_tmp"; }
unless(-e $script_folder_done){ make_path($script_folder_done) or die "Failed to create path: $script_folder_done"; }

my $active_courses_file = $script_folder .($production ? "canvas_courses.txt" : "canvas_courses_test.txt");
my $tmp_users_file = $script_folder_tmp ."users.csv";
my $tmp_enrollment_file = $script_folder_tmp ."enrollments.csv";
my $tmp_zip_file = $script_folder_tmp ."import.zip";

my $start = time();

my $db_live = DBI->connect( "DBI:mysql:database=$dbname;host=$dbhost;port=3306", $username, $password )
      || die "Could not connect to database: $DBI::errstr";

$db_live->{AutoCommit} = 0;  # enable transactions, if possible

#------------------------------------------------------------------------
print "Running on $host (". ($production ? "Production" : "Test") ."):\n";

# Read in Active Canvas courses which we want enrollment information for
my $handle;
unless (open $handle, "<:encoding(utf8)", $active_courses_file) {
   die "Could not open file '$active_courses_file': $!\n";
}
chomp(my @active_courses = <$handle>);
close $handle;

my @composite_courses = grep { index($_, '+') != -1 } @active_courses;
my @select_courses = grep { index($_, '+') == -1 } @active_courses;

foreach (@composite_courses) {

    my @main_parts = split(',', $_);
    my @sub_parts = split('\+', $main_parts[0]);
    foreach (@sub_parts) {
        push (@select_courses, $_ .",". $main_parts[1]);
    }
}

@select_courses = uniq(@select_courses);

my $active_courses_st = join ", ", @active_courses;
my $placeholders = join ", ", ("?") x @select_courses;

my $sql = "select `enroll`.user_id, `user`.`FIRST_NAME`, `user`.`LAST_NAME`, `user`.`EMAIL_LC`,`course`.ENTERPRISE_ID
    from CM_ENROLLMENT_T `enroll`
    inner join CM_ENROLLMENT_SET_T `course` on `enroll`.ENROLLMENT_SET = `course`.ENROLLMENT_SET_ID
    left join SAKAI_USER_ID_MAP `map` on `enroll`.`USER_ID` = `map`.`EID`
    left join SAKAI_USER `user` on `map`.`USER_ID` = `user`.`USER_ID`
    where `course`.ENTERPRISE_ID in ($placeholders)
    group by `enroll`.user_id";

my $get_students = $db_live->prepare($sql)
    or die "Couldn't get students: " . $db_live->errstr;

$get_students->execute(@select_courses)
    or die "Couldn't execute statement: " . $get_students->errstr;

if ($get_students->rows > 0) {

    open(my $fh_users, '>', $tmp_users_file) or die "Could not open file '$tmp_users_file' $!";
    print $fh_users "user_id,login_id,first_name,last_name,email,status\n";

    open(my $fh_enrollments, '>', $tmp_enrollment_file) or die "Could not open file '$tmp_enrollment_file' $!";
    print $fh_enrollments "course_id,user_id,role,status\n";

    while (my @data = $get_students->fetchrow_array()) {

        # user_id,login_id,first_name,last_name,email,status
        print $fh_users $data[0],",",$data[0],",",$data[1],",",$data[2],",",$data[3],",active\n";

        my @main_parts = split(',', $data[4]);
        my @course_codes = grep { index($_, $main_parts[0]) != -1 } @active_courses;

        for my $el (@course_codes) {
            print $fh_enrollments '"',$el,'",',$data[0],",student,active\n";
        }
    }

    close $fh_users;
    close $fh_enrollments;

    # add to zip file
    my $zip = Archive::Zip->new();
    $zip->addFile( $tmp_users_file, "users.csv" );
    $zip->addFile( $tmp_enrollment_file, "enrollments.csv" );

    # Save the Zip file
    unless ( $zip->writeToFileNamed($tmp_zip_file) == AZ_OK ) {
        die "Could not write zip file: $tmp_zip_file\n";
    }

    # send zip file to SIS import
    my $ua = LWP::UserAgent->new;
    my $request = HTTP::Request::Common::POST($uri,
        Content_Type => 'multipart/form-data',
        Content => [
            import_type => 'instructure_csv',
            extension => 'zip',
            diffing_data_set_identifier => 'ps:users:enrollment:2019',
            diffing_drop_status => 'inactive',
            attachment => [$tmp_zip_file]
        ]);
    $request->header("Authorization" => "Bearer $auth_token");

    my $res = $ua->request($request ) ;
    if ($res->is_success) {

        move($tmp_zip_file, 
                $script_folder_done ."import_". strftime('%Y-%m-%d_%H-%M',localtime) ."-". ($production ? "prod" : "test").".zip");
    } else {
        die "HTTP get code: ", $res->code, "\n";
    }

} else {
    print "\nNo students found in the courses: $active_courses_st\n"
}
$get_students->finish();
$db_live->disconnect();

#------------------------------------------------------------------------
my $duration = strftime("\%d \%H:\%M:\%S", gmtime(time - $start));

print "\nExecution: ", $duration, "\n";
print "------------------------------------------------------------------------\n";
