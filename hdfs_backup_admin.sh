#!/bin/bash

echo "hello"
# debug: use set -x
set -x

usage() {
    cat <<- EOF
    usage: $PROGNAME options

    Program deletes files from filesystems to release space.
    It gets config file that define fileystem paths to work on, and whitelist rules to
    keep certain files.

    OPTIONS:
       -c --config              configuration file containing the rules. use --help-config to see the syntax.
       -n --pretend             do not really delete, just how what you are going to do.
       -t --test                run unit test to check the program
       -v --verbose             Verbose. You can specify more then one -v to have more verbose
       -x --debug               debug
       -h --help                show this help
          --help-config         configuration help


    Examples:
       Run all tests:
       $PROGNAME --test all

       Run specific test:
       $PROGNAME --test test_string.sh

       Run:
       $PROGNAME --config /path/to/config/$PROGNAME.conf

       Just show what you are going to do:
       $PROGNAME -vn -c /path/to/config/$PROGNAME.conf
EOF
}

# backup_dir=$1
is_hdfs_dir() {
    local dir=$1
    return   $( hadoop fs -test -d ${dir} )
}

allow_snapshot() {
# To allow snapnshot upon a dir you must be a SUPERUSER, the owner of the dir is NOT allowed
  if $( is_hdfs_dir $1 ); then
    hdfs dfsadmin -allowSnapshot $1
  fi
}

disallow_snapshot() {
  hdfs dfsadmin -disallowSnapshot $1
}

is_snapshttable() {
#Get all the snapshottable directories where the current user has permission to take snapshtos.
  hdfs lsSnapshottableDir
}


PARAMS=""
# The number of arguments is $#
while [[ "$#" > 0  ]]; do
  case "$1" in
    allow_snapshot)
      shift
      allow_snapshot $@
      break
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *)  usage
      shift
      ;;
  esac
done

echo $PARAMS


allow_snapshot ${backup_dir}
