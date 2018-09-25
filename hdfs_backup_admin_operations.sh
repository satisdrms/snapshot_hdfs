#!/bin/bash

# debug: use set -x
set -o pipefail
set -e

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
        ## usage guide
        usage

EOF
}

# utility
is_hdfs_dir() {
  local dir=$1
  [[ -z  ${dir} ]] && echo "$FUNCNAME: ERROR empty argument" && exit 1
  hadoop fs -test -d ${dir} || { echo "$FUNCNAME: ERROR directory ${dir} does not exist" && exit 1 ;}
}

list_snapshottable_dirs() {
  echo "$FUNCNAME"
  echo "listing snapshottable directories for all users"
  #  hdfs lsSnapshottableDir
  hdfs lsSnapshottableDir |  awk '{print $NF}' | grep "^/"
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
    list_snapshottable_dirs)
      shift
      list_snapshottable_dirs $@
      exit
      ;;
    *)  usage
      exit 1
      ;;
  esac
