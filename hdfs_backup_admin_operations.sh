#!/bin/bash

# debug: use set -x
set -o pipefail

usage() {
    cat <<- EOF

  Admin  utility script for managing HDFS snapnshots
  MUST BE RUN WITH HADOOP SUPERUSER PRIVILEGES

        ## allow snapshot on a directory: ADMIN ONLY action
        allow_snapshot <dir>
        ## disallow snapshot on a directory
        disallow_snapshot <dir>
        ## list all  snapshottable directories for all users
        list_snapshottable_dirs
        ## apply retention policy on snapshotted directories
        check_and_apply_retention <dir> <number_of_snapshot_copies_to_retain>
        ## usage guide
        usage

EOF
}

# utility
is_hdfs_dir() {
  local dir=$1
  [[ -z  ${dir} ]] && echo ""$FUNCNAME": ERROR empty argument" && return 1

  hadoop fs -test -d ${dir} || { echo "$FUNCNAME: ERROR directory ${dir} does not exist" && return 1 ;}
}


#idempotent operation
# To allow snapnshot upon a dir you must be a SUPERUSER, the owner of the dir is NOT allowed
allow_snapshot() {
  echo "$FUNCNAME"
  local dir=$1
  if  is_hdfs_dir ${dir} ; then
    hdfs dfsadmin -allowSnapshot ${dir}
  fi
}

# disallow the snapnshottalbe directory, must have removed all the snapnshots
disallow_snapshot() {
  echo "$FUNCNAME"
  local dir=$1
  if  is_hdfs_dir ${dir} ; then
    hdfs dfsadmin -disallowSnapshot ${dir}
  fi
}

create_recovery_zone() {
  echo "$FUNCNAME"
}



  case "$1" in
    allow_snapshot)
      shift
      allow_snapshot $@
      exit
      ;;
    disallow_snapshot)
      shift
      disallow_snapshot $@
      exit
      ;;
    create_recovery_zone)
      shift
      create_recovery_zone $@
      exit
      ;;
    list_snapshottable_dirs)
      shift
      list_snapshottable_dirs $@
      exit
      ;;
    *)  usage
      exit 1
      ;;
  esac
