#!/bin/bash
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)"

set -e

DOCKER_REPO=""

while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "$0 - Build the kafka rebalancer"
      echo " "
      echo "$0 [options]"
      echo " "
      echo "options:"
      echo "-h, --help             show brief help"
      echo "--image                [DOCKER_REPO]    specify the url for the docker repository"
      echo "--aws                  log into ECR"
      exit 0
      ;;

    --image)
      shift
      if test $# -gt 0; then
        DOCKER_REPO="$1"
      else
        echo -e "\n\nNo repository specified\n\n"
        exit 1
      fi
      shift
      ;;

    --aws)
      shift
      AWS=true
      ;;

    *)
      break
      ;;
  esac
done

if [[ -z "$DOCKER_REPO" ]]; then
  echo -e "\n\nMissing docker repository\n\n"
  exit 1
fi

echo "AWS: $AWS"

if [ $(git describe --exact-match --tags 2> /dev/null) ] && [ "$(git diff-index --quiet HEAD --)" == "" ] ; then
  BUILD_TAG=$(git describe --exact-match --tags 2> /dev/null || git rev-parse --short HEAD)
elif [ "$CI_PIPELINE_ID" ]; then
  BUILD_TAG="ci-${CI_COMMIT_BRANCH:-$(git symbolic-ref --short HEAD)}-${CI_PIPELINE_ID}"
else
  BUILD_TAG="dev-$(git symbolic-ref --short HEAD)-$(date -u +"%Y%m%dT%H%M%SZ")"
fi


docker build \
  -t "${DOCKER_REPO}:latest" \
  "${ROOT}"

docker tag "${DOCKER_REPO}:latest" "${DOCKER_REPO}:${BUILD_TAG}"

if [[ $AWS == true ]]; then
  if [[ "$(aws --version)" = "aws-cli/2"* ]]; then
    echo -e "\nUsing aws CLI V2\N"
    aws ecr get-login-password | docker login --username AWS --password-stdin "${DOCKER_REPO}"
  else
    echo -e "\nUsing aws CLI V1\n"
    $(aws ecr get-login --no-include-email)
  fi
fi

docker push "${DOCKER_REPO}:latest"
docker push "${DOCKER_REPO}:${BUILD_TAG}"
