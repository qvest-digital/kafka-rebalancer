#!/bin/bash

IMAGE=""
SUFFIX=""

while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "$0 - Build the kafka rebalancer"
      echo " "
      echo "$0 [options]"
      echo " "
      echo "options:"
      echo "-h, --help                           show brief help"
      echo "--docker-repository [DOCKER_REPO]    specify the url for the docker repository"
      echo "--aws                                log into ECR"
      exit 0
      ;;

    --image)
      shift
      if test $# -gt 0; then
        IMAGE="$1"
      else
        echo -e "\n\nNo image specified\n\n"
        exit 1
      fi
      shift
      ;;

    --suffix)
      shift
      if test $# -gt 0; then
        SUFFIX="-$1"
      else
        echo -e "\n\nNo suffix specified\n\n"
        exit 1
      fi
      shift
      ;;

    *)
      break
      ;;
  esac
done

if [[ -z "$IMAGE" ]]; then
  echo -e "\n\nMissing image\n\n"
  exit 1
fi

kubectl run --restart=Never --image="${IMAGE}" "kafka-rebalancer$SUFFIX-$RANDOM" --image-pull-policy=Always -- "$@"
