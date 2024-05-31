#!/bin/bash

set -o errexit

# Supported/used environment variables:
# PRODUCT_NAME
# PRODUCT_VERSION

if [ -z "${PRODUCT_NAME}" ]; then
    echo "PRODUCT_NAME must be set to a non-empty string"
    exit 1
fi
if [ -z "${PRODUCT_VERSION}" ]; then
    echo "PRODUCT_VERSION must be set to a non-empty string"
    exit 1
fi

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${RELATIVE_DIR_PATH}/javaConfig.bash"

printf "\nCreating SSDLC reports\n"

declare -r SSDLC_PATH="${RELATIVE_DIR_PATH}/../build/ssdlc"
declare -r SSDLC_STATIC_ANALYSIS_REPORTS_PATH="${SSDLC_PATH}/static-analysis-reports"
mkdir "${SSDLC_PATH}"
mkdir "${SSDLC_STATIC_ANALYSIS_REPORTS_PATH}"

printf "\nCreating SpotBugs SARIF reports\n"
./gradlew -version
./gradlew -PssdlcReport.enabled=true --continue -x test -x integrationTest -x spotlessApply check scalaCheck kotlinCheck || true
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
declare -r TEMPLATE_SSDLC_REPORT_PATH="${RELATIVE_DIR_PATH}/../template_ssdlc_compliance_report.md"
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
    "${SSDLC_REPORT_PATH}"
printf "%s\n" "${SSDLC_REPORT_PATH}"

printf "\n"
