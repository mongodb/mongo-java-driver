# ${product_name} SSDLC compliance report

This report is available at
<https://d-9067613a84.awsapps.com/start/#/console?account_id=857654397073&role_name=Drivers.User&destination=https%3a%2f%2fus-west-1.console.aws.amazon.com%2fs3%2fobject%2fjava-driver-release-assets%3fregion%3dus-west-1%26bucketType%3dgeneral%26prefix%3d${product_name}%2f${product_version}%2fssdlc_compliance_report.md>.

<table>
  <tr>
    <th>Product name</th>
    <td><a href="https://github.com/mongodb/mongo-java-driver">${product_name}</a></td>
  </tr>
  <tr>
    <th>Product version</th>
    <td>${product_version}</td>
  </tr>
  <tr>
    <th>Release creator</th>
    <td>
        ${product_release_creator}
        <p>
            Refer to data in Papertrail for more details.
            There is currently no official way to serve that data.
        </p>        
    </td>
  </tr>
  <tr>
    <th>Report date, UTC</th>
    <td>${report_date_utc}</td>
  </tr>
</table>

## Process document

Blocked on <https://jira.mongodb.org/browse/JAVA-5429>.

The MongoDB SSDLC policy is available at
<https://docs.google.com/document/d/1u0m4Kj2Ny30zU74KoEFCN4L6D_FbEYCaJ3CQdCYXTMc>.

## Third-party dependency information

There are no dependencies to report vulnerabilities of.
Our [SBOM](https://docs.devprod.prod.corp.mongodb.com/mms/python/src/sbom/silkbomb/docs/CYCLONEDX/) lite
is <https://github.com/mongodb/mongo-java-driver/blob/r${product_version}/sbom.json>.

## Static analysis findings  

The static analysis findings are available at
<https://d-9067613a84.awsapps.com/start/#/console?account_id=857654397073&role_name=Drivers.User&destination=https%3a%2f%2fus-west-1.console.aws.amazon.com%2fs3%2fbuckets%2fjava-driver-release-assets%3fregion%3dus-west-1%26bucketType%3dgeneral%26prefix%3d${product_name}%2f${product_version}%2fstatic-analysis-reports%2f>.
All the findings in the aforementioned reports
are either of the MongoDB status "False Positive" or "No Fix Needed",
because code that has any other findings cannot technically get into the product.

<https://github.com/mongodb/mongo-java-driver/blob/r${product_version}/config/spotbugs/exclude.xml> may also be of interest.

## Security testing results

The testing results are available at
<${evergreen_build_url}>.

See the driver security testing summary
<https://docs.google.com/document/d/1y2K_RY4GZVXpQvv4JH_35mSzFRTawNJ3mibpvSBU8H0>
for the description of what is tested.

## Signature information

The product artifacts are signed.
The signatures can be verified by following instructions at
<https://github.com/mongodb/mongo-java-driver/releases/tag/r${product_version}>.
