#!/usr/bin/perl
use strict;

my $PG_LOG_DIR = ".";
my $HOST_NAME =  `uname -n`;
my $start_day = `date +'%Y-%m-%d'`;
my $end_day = `date +'%Y-%m-%d'`;
my $work_day;
my $output_mode = "text";
my $dur1 = 2000;
my $dur2 = 0;

chomp ( $HOST_NAME );
chomp ( $start_day );
chomp ( $end_day );

my @log_files;
my $argc = $#ARGV + 2;

&main ();

sub main
{
    if ( $ARGV[0] eq "-h" ) { &print_help (); }
    &parsing_arg ();

    $work_day = $start_day;
    &print_head ();

    for ( my $i = 0 ; $i < 100 ; $i++ )  # max 100 days
    {
        @log_files = `ls $PG_LOG_DIR/enterprisedb-$work_day*`;
        chomp ( @log_files );

        &print_slowquery_cnt ();

        if ( $work_day eq $end_day ) { last; }

        $work_day = `date -d "$work_day +1 days" +'%Y-%m-%d'`;
        chomp ( $work_day );
    }

    &print_tail ();
    exit;
}

sub print_help
{
       print "Usage : perl slowquery_hourly.pl -d [PGLOG DIR] -m1 [milli-sec] -m2 [milli-sec] -s [YYYY-MM-DD] -e [YYYY-MM-DD] -o [xml/text]\n";
       print "Arguments: \n";
       print "  -d : PGLOG directory,        default: current dir \n";
       print "  -m1 : duration ( ms ),       default: 2000 \n";
       print "  -m2 : duration ( ms ) \n";
       print "  -s : start day [YYYY-MM-DD], default: today \n";
       print "  -e : end day [YYYY-MM-DD],   default: today \n";
       print "  -o : output mode [xml/text], default: text \n";
       print "  -h : print this \n";
       print "Ex: \n";
       print "   \$ ./slowquery_hourly.pl -d /data/rm/pg_log -m1 2000 -m2 10000 -s 2013-09-17 -e 2013-09-24 -o xml > slow.xml \n";
       print "   \$ ./slowquery_hourly.pl -s 2013-09-17 \n\n";

       exit;
}

sub parsing_arg
{
    for ( my $i = 0 ; $i < $argc ; $i++ )
    {
        if ( $ARGV[$i] eq "-d" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $PG_LOG_DIR = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-m" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $dur1 = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-m1" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $dur1 = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-m2" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $dur2 = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-s" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $start_day = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-e" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $end_day = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-o" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $output_mode = $ARGV[$i];
        }
    }
}

sub print_slowquery_cnt
{
    my $hour;
    my @cnt1;
    my @cnt2;
    my $sum1;
    my $sum2;

    $sum1 = $sum2 = 0;
    for ( my $i = 0 ; $i < 24 ; $i++ ) { $cnt1[$i] = $cnt2[$i] = 0; }

    foreach my $log_file ( @log_files )
    {
        open ( LOG, $log_file ) or die "File Open Error.. $log_file \n";
        my @lines = <LOG>;
        close ( LOG );

        foreach my $line ( @lines )
        {
            if ( $line =~ $work_day && $line =~ /duration:/ )
            {
                my @log = split /ms /, $line;
                my @tmp = split /duration:/, $log[0];
                my $dur = $tmp[1];

                if ( $dur >= $dur1 )
                {
                    $hour = substr $line, 11, 2;
                    $cnt1[$hour]++;
                    $sum1++;
                }

                if ( $dur2 && $dur >= $dur2 )
                {
                    $cnt2[$hour]++;
                    $sum2++;
                }
            }
        }
    }


    if ( $output_mode eq "text" )
    {
        printf "$work_day    %5d    %5d", $dur1, $sum1 ;

        for ( my $i = 0 ; $i < 24 ; $i++ )
        {
            printf " %5d", $cnt1[$i];
        }
        print "\n";

        if ( $dur2 )
        {
            printf "              %5d    %5d", $dur2, $sum2 ;

            for ( my $i = 0 ; $i < 24 ; $i++ )
            {
                printf " %5d", $cnt2[$i];
            }
            print "\n";
        }
    }
    else
    {
        print "<slowquery host=\"$HOST_NAME\"> \n";
        print "    <date> $work_day </date> \n";
        print "    <duration> $dur1 </duration> \n";
        print "    <sum> $sum1 </sum> \n";

        for ( my $i = 0 ; $i < 24 ; $i++ )
        {
            my $hr = `printf \"%02d\" $i`;
            my $htag = "H$hr";
            print "    <$htag> $cnt1[$i] </$htag> \n";
        }
        print "</slowquery>\n";

        if ( $dur2 )
        {
            print "<slowquery host=\"$HOST_NAME\"> \n";
            print "    <date> </date> \n";
            print "    <duration> $dur2 </duration> \n";
            print "    <sum> $sum2 </sum> \n";

            for ( my $i = 0 ; $i < 24 ; $i++ )
            {
                my $hr = `printf \"%02d\" $i`;
                my $htag = "H$hr";
                print "    <$htag> $cnt2[$i] </$htag> \n";
            }
            print "</slowquery>\n";
        }
    }
}

sub print_head
{
    if ( $output_mode eq "text" )
    {
        print "\ndate       duration      sum ";
        for ( my $i = 0 ; $i < 24 ; $i++ )
        {
            printf "   %02d ", $i;
        }
        print "\n----------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
    }
    else
    {
        print "<?xml version=\"1.0\"?>\n";
        print "<root>\n";
    }
}

sub print_tail
{
    if ( $output_mode eq "text" )
    {
        print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
    }
    else
    {
        print "</root>\n";
    }
}

