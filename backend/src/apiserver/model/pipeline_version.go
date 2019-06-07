package model

// PipelineVersionStatus a label for the status of the Pipeline.
// This is intend to make pipeline creation and deletion atomic.
type PipelineVersionStatus string

const (
	PipelineVersionCreating PipelineVersionStatus = "CREATING"
	PipelineVersionReady    PipelineVersionStatus = "READY"
	PipelineVersionDeleting PipelineVersionStatus = "DELETING"
)

type PipelineVersion struct {
	UUID           string `gorm:"column:VersionUUID; not null; primary_key"`
	CreatedAtInSec int64  `gorm:"column:VersionCreatedAtInSec; not null"`
	Name           string `gorm:"column:VersionName; not null"`
	/* Set size to 65535 so it will be stored as longtext. https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html */
	Parameters string                `gorm:"column:VersionParameters; not null; size:65535"`
	PipelineId string                `gorm:"column:PipelineId; not null"`
	Status     PipelineVersionStatus `gorm:"column:VersionStatus; not null"`
	CodeSource
}

type CodeSource struct {
	RepoName  string `gorm:"column:RepoName"`
	CommitSHA string `gorm:"column:CommitSHA"`
}