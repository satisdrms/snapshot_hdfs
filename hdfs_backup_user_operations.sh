#!/bin/bash
# set -x
# include set -e at the top. This tells bash that it
#should exit the script if any statement returns a non-true return value.
set -e    # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

usage() {
    cat <<- EOF

  User utility script for managing HDFS snapnshots
    ## list all the snapshot directories availalble for user ${USER}
    list_snapshottable_dirs
    ## creates a snapshot for a directory,user ${USER} must be owner of this directory
    create_snapshot <dir>
    ## list all  availalble snapnshots for a directory
    list_all_snapshots <dir>
    ## apply retention policy on snapshotted directories
    check_and_apply_retention <dir> <number_of_snapshot_copies_to_retain>
    ## usage guide
    usage

EOF
}

DEFAULT_NB_SNAPSHOTS=7

###################################
###################################
###utlities###

is_positive_integer() {
    [[ $# == 1 ]] && [[ $1 =~ ^[0-9]+$ ]] && [[ $1 > 0 ]] || \
    { echo "$FUNCNAME: ERROR $1 must be a valid integer and > 0" ; exit 1 ;}
}


is_hdfs_dir() {
  local dir=$1
  { [[ -z  ${dir} ]] && echo "$FUNCNAME: ERROR empty argument" && exit 1; }

  hadoop fs -test -d ${dir} || { echo "$FUNCNAME: ERROR directory ${dir} does not exist" && exit 1 ;}

}


is_snapshottable() {
  #set -x
  local dir=$1
  is_hdfs_dir ${dir}

  local -a  list_user_snapshots=( $( list_snapshottable_dirs ) )
  #check if dir belongs to snapshottable dirs
  #check if a bash variable is unset or set to the empty string
   [[ ! -z $( printf '%s\n' ${list_user_snapshots[@]} | grep -P "^${dir}$" ) ]] || \
   { echo "${dir} does not belong to the list of snapshottable directories for user ${USER}" && exit 1;}

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

  local res=$(hdfs lsSnapshottableDir |  awk '{print $NF}' | grep "^/")
  { [[ -z ${res} ]] && echo "user ${USER} has no snapshottable directory" ;} || echo ${res}

}

create_snapshot () {
  #  hdfs dfs -createSnapshot <path> [<snapshotName>]
  [[ $# == 1 ]] || \
  { echo "$FUNCNAME: Please provide a path "; exit 1 ;}
  hdfs dfs -createSnapshot $1
}

# get list of retained snapnshots in chronological order
list_all_snapshots () {
  local dir=$1
  is_snapshottable ${dir}
  local res=$(hdfs dfs -ls -t  ${dir}/.snapshot | awk '{print $NF}' | grep "^/")
  [[ -z ${res} ]]  && "$FUNCNAME: ERROR no snapshots created yet for directory ${dir}" && exit 1 || echo ${res}

}

check_and_apply_retention() {

   [[ $# == 1 || $# == 2  ]]  || ( usage && exit 1 )

  local dir=$1  ; local nb_snapshots_to_retain=$2
  [[ -z ${nb_snapshots_to_retain} ]] && nb_snapshots_to_retain=${DEFAULT_NB_SNAPSHOTS} && \
  echo "INFO number of snapshots to retain not set, applying default retention= ${DEFAULT_NB_SNAPSHOTS}"

  # check
  is_snapshottable ${dir} && is_positive_integer ${nb_snapshots_to_retain}

  # core
  local arr_existing_snapshots=( $(list_all_snapshots ${dir}) )
  local nb_existing_snapshots=${#arr_existing_snapshots[@]}

  if [[ ${nb_existing_snapshots} > ${nb_snapshots_to_retain} ]]; then
    local nb_snapshots_to_remove=$((nb_existing_snapshots - nb_snapshots_to_retain ))
    local arr_snapshots_to_remove=( ${arr_existing_snapshots[@]:0:$nb_snapshots_to_remove} )
    #echo "arr_snapshots_to_remove" $arr_snapshots_to_remove

    for snap_to_remove in ${arr_snapshots_to_remove[@]}; do
      snap_version_name=$( echo ${snap_to_remove} | awk  -F  "/"  '{ print $NF }' )
      #echo "snap to remove" ${snap_to_remove}
      # echo "snap_version_name to remove $snap_version_name"
      #hdfs dfs -ls ${snap_to_remove}
      hdfs dfs -deleteSnapshot ${dir} ${snap_version_name}
    done

  else
    echo "INFO no additional snapnshots to remove, ${nb_existing_snapshots} snapnshots exists for ${dir} directory  "
    echo "INFO list of existing_snapshots"
    printf "%s\n"  "${arr_existing_snapshots[@]}"
    exit 0
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
  list_all_snapshots)
    shift
    list_all_snapshots $@
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
