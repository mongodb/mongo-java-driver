import com.typesafe.sbt.SbtGit._
import sbt._

object Versioning {

  val snapshotSuffix = "-SNAPSHOT"
  val releasedVersion = """^r?([0-9\.]+)$""".r
  val releasedCandidateVersion = """^r?([0-9\.]+-rc\d+)$""".r
  val betaVersion = """^r?([0-9\.]+-beta\d+)$""".r
  val snapshotVersion = """^r?[0-9\.]+(.*)$""".r

  def settings(baseVersion: String): Seq[Def.Setting[_]] = Seq(
    git.baseVersion := baseVersion,
    git.uncommittedSignifier := None,
    git.useGitDescribe := true,
    git.formattedShaVersion := git.gitHeadCommit.value map(sha => s"$baseVersion-${sha take 7}$snapshotSuffix"),
    git.gitTagToVersionNumber := {
      case releasedVersion(v) => Some(v)
      case releasedCandidateVersion(rc) => Some(rc)
      case betaVersion(beta) => Some(beta)
      case snapshotVersion(v) => Some(s"$baseVersion$v$snapshotSuffix")
      case _ => None
    }
  )

}
