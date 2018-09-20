#!/bin/bash
# set -x

###################################
###################################
###utlities###
usage() {
    cat <<- EOF
User utility script for managing HDFS snapnshots
    ## list all the snapshot directories availalble for user ${USER}
    list_snapshottable_dirs
    ## creates a snapshot for a directory
    create_snapshot <dir>
    ## list all  availalble snapnshots for a directory
    list_available_snapshots <dir>
    ## apply retention policy on snapshotted directories
    check_and_apply_retention <dir> <number_of_snapshot_copies_to_retain>
    ## usage guide
    usage

EOF
}


is_positive_integer() {
    [[ $# == 1 ]] && [[ $1 =~ ^[0-9]+$ ]] && [[ $1 > 0 ]]
}

is_hdfs_dir() {
  local dir=$1
  hadoop fs -test -d ${dir}
}

is_snapshottable() {

  local dir=$1

  is_hdfs_dir ${dir} || { echo "ERROR: ${dir} does not exist" ;return 1 ;}

  echo "INFO verifying ${dir} is snapshottable"
    #  hdfs lsSnapshottableDir
  local  list_user_snapshots=$( list_snapshottable_dirs )
  #check if dir belongs to snapshottable dirs
  #check if a bash variable is unset or set to the empty string
  if  [[ ! -z ${list_user_snapshots} ]] && [[ ! -z $( echo $list_user_snapshots | egrep "^${dir}$" ) ]]; then
    echo "INFO OK ${dir} is a snapshottable directory" >&2
    return 0
  else
    echo "ERROR ${dir} is not a snapshottable directory" >&2
    return 1
  fi
}

delete_snapshot() {
  #  hdfs dfs -deleteSnapshot <path> <snapshotName>
  local path=$1 ; local snapshot_name=$2
   hdfs dfs -deleteSnapshot ${path} ${snapshot_name} >&2
}

###########################################
###########################################
### user operations ###

list_snapshottable_dirs() {
  #  hdfs lsSnapshottableDir
  hdfs lsSnapshottableDir |  awk '{print $NF}' | grep "^/"
}

create_snapshot () {
  #  hdfs dfs -createSnapshot <path> [<snapshotName>]
  hdfs dfs -createSnapshot $1
}

# get list of retained snapnshots in chronological order
list_available_snapshots () {

  local dir=$1
  is_snapshottable ${dir}   || exit 1 ;
  hdfs dfs -ls -t  ${dir}/.snapshot | awk '{print $NF}' | grep "^/"

}

check_and_apply_retention() {

  local dir=$1  ; local nb_snapshots_to_retain=$2
  # check
  is_snapshottable ${dir} || {  echo "EROOR: dir ${dir}  is not snapshottable" ; return 1 ;}
  # quiv to   if ! is_hdfs_dir ${dir}; then  echo "EROOR: dir ${dir}  doesnt exist" ;  return 1 ; fi
  is_hdfs_dir ${dir}  || { echo "EROOR: dir ${dir}  doesnt exist" ; return 1 ;}
  is_positive_integer ${nb_snapshots_to_retain} || { echo "ERROR: ${nb_snapshots_to_retain} must be a valid integer and > 0" ;return 1 ;}

  # core
  local arr_existing_snapshots=( list_available_snapshots ${dir} )
  local nb_existing_snapshots=${#arr_existing_snapshots[@]}

  if [[ ${nb_existing_snapshots} > ${nb_snapshots_to_retain} ]]; then
    local nb_snapshots_to_remove=$((nb_existing_snapshots - nb_snapshots_to_retain ))
    local arr_snapshots_to_remove=( ${arr_existing_snapshots[@]:0:$nb_snapshots_to_remove} )

    for snap_to_remove in ${arr_snapshots_to_remove[@]}; do
      snap_version_name=$( echo ${snap_to_remove} | awk  -F  "/"  '{ print $NF }' )
      echo "snap to remove" ${snap_to_remove}
      echo "snap_version_name to remove $snap_version_name"
      #delete_snapshot ${dir} ${snap_version_name}
      hdfs dfs -ls ${snap_to_remove}
    done

  else
    echo "INFO: no additional snapnshots to remove"
    return 0
  fi

}


[[ $# == 0 ]] && usage && exit 1 ;

case "$1" in
  list_snapshottable_dirs)
    shift
    list_snapshottable_dirs $@
    ;;
  create_snapshot)
    shift
    create_snapshot $@
    ;;
    delete_snapshot)
      shift
      delete_snapshot $@
      ;;
  list_available_snapshots)
    shift
    list_available_snapshots $@
    ;;
  list_all_snapshttable_dirs)
    shift
    list_all_snapshttable_dirs $@
    ;;
  is_snapshottable)
      shift
    is_snapshottable $@
      ;;
  check_and_apply_retention)
    shift
    check_and_apply_retention $@
    ;;
  -h | --help) usage
    exit 0
    ;;
  *)  usage
    exit 1;
    ;;
esac
