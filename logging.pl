#!/usr/bin/perl
use strict;

my $PG_VERSION = substr( `pg_ctl --version | awk '{print \$3}'`, 0, 3 );
my $HOST_NAME =  `uname -n`; chomp ( $HOST_NAME );

my $cdate;
my $log_file;         # logfile name

my $port = 5444;      # port
my $dbname = 'edb';   # db name : -d
my $term = 60;        # check cycle : -t
my $log_dir;          # log directory : -l

#
# TRACK ITMES
#
my $max_conn;
my $cur_conn;
my $act_conn;
my $waiting;
my $slow_query;
my $cpu;
my $mem;

my $xact_commit;
my $xact_rollback;
my $blks_read;
my $blks_hit;
my $tup_returned;
my $tup_fetched;
my $tup_inserted;
my $tup_updated;
my $tup_deleted;
my $deadlocks;

my $cur_xact_commit;
my $cur_xact_rollback;
my $cur_blks_read;
my $cur_blks_hit;
my $cur_tup_returned;
my $cur_tup_fetched;
my $cur_tup_inserted;
my $cur_tup_updated;
my $cur_tup_deleted;
my $cur_deadlocks;

my $prev_xact_commit;
my $prev_xact_rollback;
my $prev_blks_read;
my $prev_blks_hit;
my $prev_tup_returned;
my $prev_tup_fetched;
my $prev_tup_inserted;
my $prev_tup_updated;
my $prev_tup_deleted;
my $prev_deadlocks;


my $argc = $#ARGV + 2;

&main ();

sub main
{
    if ( $argc eq 1 or  $ARGV[0] eq "-h" or $ARGV[0] eq "--h"
        or $ARGV[0] eq "-help"or $ARGV[0] eq "--help")
    {
        &print_help ();
    }

    &parsing_arg ();

    $max_conn = `psql -p $port -c "show max_connections;" | sed -n '3,1p'`;

    &scan_pg_stat_database ();
    &set_prev_pg_stat_database ();

    my $fdate = `date +'%Y%m%d'`; chomp ( $fdate );

    my $log_path = '';
    if ( $log_dir ) { $log_path = $log_dir . "/"; }

    $log_file = $log_path . $HOST_NAME . "_" . $fdate . ".log";
    open ( TXT, ">$log_file" ) or die "File Create Error.. $log_file \n";

    while ( 1 )
    {
        #
        # Log file switching by daily
        #
        my $today = `date +'%Y%m%d'`; chomp ( $today);
        if ( $fdate ne $today )
        {
            $fdate = $today;
            close ( TXT );

            $log_file = $log_path . $HOST_NAME . "_" . $fdate . ".log";
            open ( TXT, ">$log_file" ) or die "File Create Error.. $log_file \n";
        }

        #
        # get informations
        #
        &print_head ();
        &get_track ();
        &get_session_info ();
        &get_slow_query ();
        &get_deadlock ();
        &get_pg_locks ();
        &get_process_status ();
        &print_tail ();

        sleep $term;
    }

    exit;
}


sub print_help
{
    print "Usage : logging.pl -t [seconds] -d [db_name] -p [port] -l [log_dir]\n";
    print "    Arguments : \n";
    print "       -t : check period ( second ) - default : 60 secs \n";
    print "       -d : database name           - default : edb \n";
    print "       -p : port                    - default : 5444 \n";
    print "       -l : log dir                 - default : current dir \n";
    exit;
}

sub parsing_arg
{
    for ( my $i = 0 ; $i < $argc ; $i++ )
    {
        if ( $ARGV[$i] eq "-t" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $term = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-d" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $dbname = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-p" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $port = $ARGV[$i];
        }
        elsif ( $ARGV[$i] eq "-l" )
        {
            $i++;
            if ( $i eq $argc ) { last; }
            $log_dir = $ARGV[$i];
        }
    }
}

sub get_track
{
    $cdate = `date +'%Y-%m-%d %H:%M:%S'`;

    chomp ( $cdate );

    $max_conn = `psql -p $port -c "show max_connections;" | sed -n '3,1p'`;
    $cur_conn = `psql -p $port -c  "select count(*) from pg_stat_activity;" | sed -n '3,1p'`;

    if ( $PG_VERSION ge "9.2" )
    {
        $act_conn = `psql -p $port -c "select count(*) from pg_stat_activity where state != 'idle';" | sed -n '3,1p'`;
    }
    else
    {
        $act_conn = `psql -p $port -c "select count(*) from pg_stat_activity where current_query != '<IDLE>';" | sed -n '3,1p'`;
    }

    $waiting = `psql -p $port -c "select count(*) from pg_stat_activity where waiting = 't';" | sed -n '3,1p'`;

    $slow_query = `psql -p $port -c "select count(*) from pg_stat_activity where query_start < now() - interval '1 minute' and datname='$dbname';" -t -A`;

    chomp ( $slow_query );
    chomp ( $max_conn );
    chomp ( $cur_conn );
    chomp ( $act_conn );
    chomp ( $waiting);

    &get_cpu_mem_usage ();

    &scan_pg_stat_database ();
    &get_xtract ();
    &set_prev_pg_stat_database ();

    printf TXT "----------------------------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              Track Data \n";
    printf TXT "----------------------------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "          date/time    cpu memory session active wait commit rollback blks_read    blks_hit    fetched inserted updated deleted deadlocks slow_query\n";
    printf TXT "----------------------------------------------------------------------------------------------------------------------------------------------------\n";

    printf TXT "%s %5.1f%% %5.1f%% %7d %6d %4d %6d %8d %9d %11d %10d %8d %7d %7d %9d %10d\n", $cdate, $cpu, $mem, $cur_conn, $act_conn, $waiting, $xact_commit, $xact_rollback, $blks_read, $blks_hit, $tup_fetched, $tup_inserted, $tup_updated, $tup_deleted, $deadlocks, $slow_query;
}

sub scan_pg_stat_database
{
    my $res = `psql -p $port -c  "select xact_commit,xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,tup_inserted,tup_updated,tup_deleted from pg_stat_database where datname='$dbname';" -t -A`;

    chomp ( $res );
    my @values = split /\|/, $res;


    $cur_xact_commit = $values[0];
    $cur_xact_rollback = $values[1];
    $cur_blks_read = $values[2];
    $cur_blks_hit = $values[3];
    $cur_tup_returned = $values[4];
    $cur_tup_fetched = $values[5];
    $cur_tup_inserted = $values[6];
    $cur_tup_updated = $values[7];
    $cur_tup_deleted = $values[8];
    $cur_deadlocks = $values[9];
}

sub set_prev_pg_stat_database
{
    $prev_xact_commit = $cur_xact_commit;
    $prev_xact_rollback = $cur_xact_rollback;
    $prev_blks_read = $cur_blks_read;
    $prev_blks_hit = $cur_blks_hit;
    $prev_tup_returned = $cur_tup_returned;
    $prev_tup_fetched = $cur_tup_fetched;
    $prev_tup_inserted = $cur_tup_inserted;
    $prev_tup_updated = $cur_tup_updated;
    $prev_tup_deleted = $cur_tup_deleted;
    $prev_deadlocks = $cur_deadlocks;
}

sub get_xtract
{
    $xact_commit = $cur_xact_commit - $prev_xact_commit;
    $xact_rollback = $cur_xact_rollback - $prev_xact_rollback;
    $blks_read = $cur_blks_read - $prev_blks_read;
    $blks_hit = $cur_blks_hit - $prev_blks_hit;
    $tup_returned = $cur_tup_returned - $prev_tup_returned;
    $tup_fetched = $cur_tup_fetched - $prev_tup_fetched;
    $tup_inserted = $cur_tup_inserted - $prev_tup_inserted;
    $tup_updated = $cur_tup_updated - $prev_tup_updated;
    $tup_deleted = $cur_tup_deleted - $prev_tup_deleted;
    $deadlocks = $cur_deadlocks - $prev_deadlocks;
}

sub get_cpu_mem_usage
{
    $cpu = `top -b -n 1 | grep -i cpu\\(s\\) | awk '{print \$5}' | tr -d "%id," | awk '{print 100-\$1}'`;
    my $mem_total = `free -m | grep "Mem:" | awk '{print \$2}'`;
    my $mem_free = `free -m | grep "cache:" | awk '{print \$4}'`;

    chomp ( $cpu );
    chomp ( $mem_total );
    chomp ( $mem_free );

    my $mem_used = $mem_total - $mem_free;
    $mem = sprintf ( "%.1f", $mem_used / $mem_total * 100 );
}

sub get_process_status
{
    my $ps = `ps auxf | grep postgres`;

    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              PPAS DB Process Status                                              \n";
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";

    print TXT "$ps";
}

sub get_slow_query
{
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              Slow query ( 10 minute )                                            \n";
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";

    my $slow_query;
    if ( $PG_VERSION ge "9.2" )
    {
        $slow_query = `psql -p $port -t -c "select pid, query_start, substring(query,1,50) as query from pg_stat_activity where query_start < now() - interval '10 minute' order by query_start"`;
    }
    else
    {
        $slow_query = `psql -p $port -t -c "select procpid, query_start, substring(current_query,1,50) as query from pg_stat_activity where query_start < now() - interval '10 minute' order by query_start"`;
    }

    chomp ( $slow_query );
    chomp ( $slow_query );

    print TXT "$slow_query\n";
}

sub get_deadlock
{
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              Dead Lock Analyze                                            \n";
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";

    my $dead_lock;
    if ( $PG_VERSION ge "9.2" )
    {
        $dead_lock = `psql -p $port -c "SELECT blockingl.relation::regclass,
                                      blockeda.pid AS blocked_pid, blockeda.query AS blocked_query,
                                      blockedl.mode AS blocked_mode,
                                      blockinga.pid AS blocking_pid, blockinga.query AS blocking_query,
                                      blockingl.mode AS blocking_mode
                                FROM pg_catalog.pg_locks blockedl
                                JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
                                JOIN pg_catalog.pg_locks blockingl ON(blockingl.relation=blockedl.relation
                                      AND blockingl.locktype=blockedl.locktype AND blockedl.pid != blockingl.pid)
                                JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
                                WHERE NOT blockedl.granted AND blockinga.datname='$dbname'"`;
    }
    else
    {
        $dead_lock = `psql -p $port -c "select    blockingl.relation::regclass,
                            blockeda.procpid AS blocked_pid,
                            blockeda.current_query AS blocked_query,
                            blockedl.mode AS blocked_mode,
                            blockinga.procpid AS blocking_pid,
                            blockinga.current_query AS blocking_query,
                            blockingl.mode AS blocking_mode
                    FROM pg_catalog.pg_locks blockedl
                    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.procpid
                    JOIN pg_catalog.pg_locks blockingl ON(blockingl.relation=blockedl.relation
                                 AND blockingl.locktype=blockedl.locktype
                                AND blockedl.pid != blockingl.pid)
                    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.procpid
                     WHERE NOT blockedl.granted AND blockinga.datname='$dbname'"`;
    }

    chomp ( $dead_lock );
    chomp ( $dead_lock );
    print TXT "$dead_lock\n";
}

sub get_session_info
{
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              session information\n";
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";

    my $session_info = `psql -p $port -c "select * from pg_stat_activity order by query_start"`;

    chomp ( $session_info );
    chomp ( $session_info );

    print TXT "$session_info\n";
}

sub get_pg_locks
{
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";
    printf TXT "                                              relation pg_locks \n";
    printf TXT "-------------------------------------------------------------------------------------------------------------------------------\n";

    my $pg_locks = `psql -p $port -c "select * from pg_locks"`;
    chomp ( $pg_locks );
    chomp ( $pg_locks );

    print TXT "$pg_locks\n";
}

sub print_head
{
    my $check_date = `date +'%Y-%m-%d %H:%M:%S'`; chomp ( $check_date );

    printf TXT "===============================================================================================================================\n";
    printf TXT "Host : $HOST_NAME   Check Date : $check_date \n";
    printf TXT "===============================================================================================================================\n";
}

sub print_tail
{
    printf TXT "\n\n";
}

