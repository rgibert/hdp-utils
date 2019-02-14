#!/bin/bash

set -ue

print_help() {
  if [[ -n ${1:-} ]]; then
    exit_code=${1}
  else
    exit_code=0
  fi

  echo "usage: $(basename "${0}") [OPTIONS]"
  echo ""
  echo "Script to sync LDAP users and/or groups with Ambari"
  echo ""
  echo "Required parameters:"
  echo " -u ambari_user    Ambari admin user name"
  echo " -s ambari_uri     Base URI for the Ambari server"
  echo ""
  echo "Optional parameters:"
  echo " -p ambari_pass    Ambari admin user's password"
  echo " -f users          Comma separated list of users to sync (at least one of -f, -a, or -g must be provided)"
  echo " -a admin_users    Comma separated list of users to sync and be made admins (at least one of -f, -a, or -g must be provided)"
  echo " -g groups         Comma separated list of groups to sync (at least one of -f, -a, or -g must be provided)"
  echo " -h                Help screen"
  echo ""

  exit ${exit_code}
}

print_err() {
  echo "ERROR: ${1}"
  echo ""
  print_help 1
}

while getopts ":u:p:s:f:g:a:h" OPT; do
  shopt -s nocasematch
  case ${OPT} in
    u)
      ambari_user="${OPTARG}"
      ;;
    p)
      ambari_pass="${OPTARG}"
      ;;
    s)
      ambari_uri="${OPTARG}"
      ;;
    f)
      user_list="${OPTARG}"
      ;;
    g)
      group_list="${OPTARG}"
      ;;
    a)
      admin_user_list="${OPTARG}"
      ;;
    h)
      print_help
      ;;
  esac
done

# Verify required parameters were provided
if [[ -z ${ambari_user:-} ]]; then
  print_err "-u not specified and is required"
elif [[ -z ${ambari_uri:-} ]]; then
  print_err "-s not specified and is required"
elif [[ -z ${user_list:-} ]] && [[ -z ${group_list:-} ]] && [[ -z ${admin_user_list:-} ]]; then
  print_err "-f or -g (or both) must be provided"
fi

if [[ -n "${ambari_pass:-}" ]]; then
  ambari_pass_arg=":${ambari_pass}"
else
  ambari_pass_arg=""
fi

curl_sync() {
  curl \
    -s \
    -k \
    -H 'X-Requested-By: ambari' \
    -u "${ambari_user}${ambari_pass_arg}" \
    -X POST \
    -d "[{\"Event\": {\"specs\": [{\"principal_type\": \"${1}\", \"sync_type\": \"specific\", \"names\": \"${2}\"}]}}]" \
    "${ambari_uri}/api/v1/ldap_sync_events"
}

curl_make_admin() {
  IFS=","
  for user in ${1}; do
    curl \
      -s \
      -k \
      -H 'X-Requested-By: ambari' \
      -u "${ambari_user}${ambari_pass_arg}" \
      -X PUT \
      -d '{"Users" : {"admin" : "true"}}' \
      "${ambari_uri}/api/v1/users/${user}"
  done
  IFS=" "
}

if [[ -n "${user_list:-}" ]]; then
  curl_sync "users" "${user_list},${admin_user_list}"
fi

if [[ -n "${admin_user_list:-}" ]]; then
  curl_sync "users" "${admin_user_list}"
  curl_make_admin  "${admin_user_list}"
fi

if [[ -n "${group_list:-}" ]]; then
  curl_sync "groups" "${group_list}"
fi

