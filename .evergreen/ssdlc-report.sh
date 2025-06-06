#!/usr/bin/env bash

set -eu

# Supported/used environment variables:
# PRODUCT_NAME
# PRODUCT_VERSION
# EVERGREEN_VERSION_ID

if [ -z "${PRODUCT_NAME}" ]; then
    printf "\nPRODUCT_NAME must be set to a non-empty string\n"
    exit 1
fi
if [ -z "${PRODUCT_VERSION}" ]; then
    printf "\nPRODUCT_VERSION must be set to a non-empty string\n"
    exit 1
fi
if [ -z "${EVERGREEN_VERSION_ID}" ]; then
    printf "\EVERGREEN_VERSION_ID must be set to a non-empty string\n"
    exit 1
fi

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/setup-env.bash"

printf "\nCreating SSDLC reports\n"
printf "\nProduct name: %s\n" "${PRODUCT_NAME}"
printf "\nProduct version: %s\n" "${PRODUCT_VERSION}"

declare -r SSDLC_PATH="${RELATIVE_DIR_PATH}/../build/ssdlc"
declare -r SSDLC_STATIC_ANALYSIS_REPORTS_PATH="${SSDLC_PATH}/static-analysis-reports"
mkdir "${SSDLC_PATH}"
mkdir "${SSDLC_STATIC_ANALYSIS_REPORTS_PATH}"

declare -r EVERGREEN_PROJECT_NAME_PREFIX="${PRODUCT_NAME//-/_}"
declare -r EVERGREEN_BUILD_URL_PREFIX="https://spruce.mongodb.com/version"
declare -r GIT_TAG="r${PRODUCT_VERSION}"
GIT_COMMIT_HASH="$(git rev-list --ignore-missing -n 1 "${GIT_TAG}")"
set +e
    GIT_BRANCH_DEFAULT="$(git branch -a --contains "${GIT_TAG}" | grep 'main$')"
    GIT_BRANCH_PATCH="$(git branch -a --contains "${GIT_TAG}" | grep '\.x$')"
set -e
if [ -n "${GIT_BRANCH_DEFAULT}" ]; then
    declare -r EVERGREEN_BUILD_URL="${EVERGREEN_BUILD_URL_PREFIX}/${EVERGREEN_PROJECT_NAME_PREFIX}_${GIT_COMMIT_HASH}"
elif [ -n "${GIT_BRANCH_PATCH}" ]; then
    # strip out the patch version
    declare -r EVERGREEN_PROJECT_NAME_SUFFIX="${PRODUCT_VERSION%.*}"
    declare -r EVERGREEN_BUILD_URL="${EVERGREEN_BUILD_URL_PREFIX}/${EVERGREEN_PROJECT_NAME_PREFIX}_${EVERGREEN_PROJECT_NAME_SUFFIX}_${GIT_COMMIT_HASH}"
elif [[ "${PRODUCT_NAME}" == *'-snapshot' ]]; then
    declare -r EVERGREEN_BUILD_URL="${EVERGREEN_BUILD_URL_PREFIX}/${EVERGREEN_VERSION_ID}"
else
    printf "\nFailed to compute EVERGREEN_BUILD_URL\n"
    exit 1
fi
printf "\nEvergreen build URL: %s\n" "${EVERGREEN_BUILD_URL}"

PRODUCT_RELEASE_CREATOR="$(git log --ignore-missing "${GIT_TAG}"^.."${GIT_TAG}" --simplify-by-decoration --pretty='format:%aN')"
printf "\nProduct release creator: %s\n" "${PRODUCT_RELEASE_CREATOR}"

printf "\nCreating SpotBugs SARIF reports\n"
./gradlew -version
set +e
    # This `gradlew` command is expected to exit with a non-zero exit status,
    # because it reports all the findings that we normally explicitly exclude as "No Fix Needed"/"False Positive".
    ./gradlew -PssdlcReport.enabled=true --continue -x test -x integrationTest -x spotlessApply check scalaCheck
set -e
printf "\nSpotBugs created the following SARIF reports\n"
IFS=$'\n'
declare -a SARIF_PATHS=($(find "${RELATIVE_DIR_PATH}/.." -path "*/spotbugs/*.sarif"))
unset IFS
for SARIF_PATH in "${SARIF_PATHS[@]}"; do
    GRADLE_PROJECT_NAME="$(basename "$(dirname "$(dirname "$(dirname "$(dirname "${SARIF_PATH}")")")")")"
    NEW_SARIF_PATH="${SSDLC_STATIC_ANALYSIS_REPORTS_PATH}/${GRADLE_PROJECT_NAME}_$(basename "${SARIF_PATH}")"
    cp "${SARIF_PATH}" "${NEW_SARIF_PATH}"
    printf "%s\n" "${NEW_SARIF_PATH}"
done

printf "\nCreating SSDLC compliance report\n"
declare -r TEMPLATE_SSDLC_REPORT_PATH="${RELATIVE_DIR_PATH}/template_ssdlc_compliance_report.md"
declare -r SSDLC_REPORT_PATH="${SSDLC_PATH}/ssdlc_compliance_report.md"
cp "${TEMPLATE_SSDLC_REPORT_PATH}" "${SSDLC_REPORT_PATH}"
declare -a SED_EDIT_IN_PLACE_OPTION
if [[ "$OSTYPE" == "darwin"* ]]; then
    SED_EDIT_IN_PLACE_OPTION=(-i '')
else
    SED_EDIT_IN_PLACE_OPTION=(-i)
fi
sed "${SED_EDIT_IN_PLACE_OPTION[@]}" \
    -e "s/\${product_name}/${PRODUCT_NAME}/g" \
    -e "s/\${product_version}/${PRODUCT_VERSION}/g" \
    -e "s/\${report_date_utc}/$(date -u +%Y-%m-%d)/g" \
    -e "s/\${product_release_creator}/${PRODUCT_RELEASE_CREATOR}/g" \
    -e "s>\${evergreen_build_url}>${EVERGREEN_BUILD_URL}>g" \
    "${SSDLC_REPORT_PATH}"
printf "%s\n" "${SSDLC_REPORT_PATH}"

printf "\n"
