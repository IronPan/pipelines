// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/list"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
)

type PipelineStoreInterface interface {
	ListPipelines(opts *list.Options) ([]*model.Pipeline, int, string, error)
	GetPipeline(pipelineId string) (*model.Pipeline, error)
	GetPipelineWithStatus(id string, status model.PipelineStatus) (*model.Pipeline, error)
	DeletePipeline(pipelineId string) error
	CreatePipeline(*model.Pipeline) (*model.Pipeline, error)
	UpdatePipelineStatus(string, model.PipelineStatus) error

	CreatePipelineVersion(*model.PipelineVersion) (*model.PipelineVersion, error)
	GetPipelineVersion(pipelineId string, versionId string) (*model.PipelineVersion, error)
	// TODO paginating the list pipeline versions
	ListPipelineVersions(pipelineId string) ([]*model.PipelineVersion, error)
	UpdatePipelineVersionStatus(string, model.PipelineVersionStatus) error
}

type PipelineStore struct {
	db   *DB
	time util.TimeInterface
	uuid util.UUIDGeneratorInterface
}

// Runs two SQL queries in a transaction to return a list of matching pipelines, as well as their
// total_size. The total_size does not reflect the page size.
func (s *PipelineStore) ListPipelines(opts *list.Options) ([]*model.Pipeline, int, string, error) {
	errorF := func(err error) ([]*model.Pipeline, int, string, error) {
		return nil, 0, "", util.NewInternalServerError(err, "Failed to list pipelines: %v", err)
	}

	buildQuery := func(sqlBuilder sq.SelectBuilder) sq.SelectBuilder {
		return sqlBuilder.From("pipelines").Where(sq.Eq{"Status": model.PipelineReady})
	}

	sqlBuilder := buildQuery(sq.Select("*"))
	// SQL for row list
	rowsSql, rowsArgs, err := opts.AddPaginationToSelect(sqlBuilder).
		LeftJoin("pipeline_versions on pipelines.DefaultVersionId=pipeline_versions.UUID").
		ToSql()
	if err != nil {
		return errorF(err)
	}

	// SQL for getting total size. This matches the query to get all the rows above, in order
	// to do the same filter, but counts instead of scanning the rows.
	sizeSql, sizeArgs, err := buildQuery(sq.Select("count(*)")).ToSql()
	if err != nil {
		return errorF(err)
	}

	// Use a transaction to make sure we're returning the total_size of the same rows queried
	tx, err := s.db.Begin()
	if err != nil {
		glog.Errorf("Failed to start transaction to list pipelines")
		return errorF(err)
	}

	rows, err := tx.Query(rowsSql, rowsArgs...)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	pipelines, err := s.scanPipelineRows(rows)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	rows.Close()

	sizeRow, err := tx.Query(sizeSql, sizeArgs...)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	total_size, err := list.ScanRowToTotalSize(sizeRow)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	sizeRow.Close()

	err = tx.Commit()
	if err != nil {
		glog.Errorf("Failed to commit transaction to list pipelines")
		return errorF(err)
	}

	if len(pipelines) <= opts.PageSize {
		return pipelines, total_size, "", nil
	}

	npt, err := opts.NextPageToken(pipelines[opts.PageSize])
	return pipelines[:opts.PageSize], total_size, npt, err
}

func (s *PipelineStore) scanPipelineRows(rows *sql.Rows) ([]*model.Pipeline, error) {
	var pipelines []*model.Pipeline
	for rows.Next() {
		var uuid, name, description, defaultVersionId, versionUuid, versionName, versionParameters,
		versionPipelineId, versionRepoName, versionCommitSha string
		var createdAtInSec, versionCreatedAtInSec int64
		var status model.PipelineStatus
		var versionStatus model.PipelineVersionStatus
		if err := rows.Scan(&uuid, &createdAtInSec, &name, &description, &status, &defaultVersionId,
			&versionUuid, &versionCreatedAtInSec, &versionName, &versionParameters, &versionPipelineId,
			&versionRepoName, &versionCommitSha, &versionStatus); err != nil {
			return nil, err
		}
		pipelines = append(pipelines, &model.Pipeline{
			UUID:           uuid,
			CreatedAtInSec: createdAtInSec,
			Name:           name,
			Description:    description,
			Status:         status,
			DefaultVersion: &model.PipelineVersion{
				UUID:           versionUuid,
				CreatedAtInSec: versionCreatedAtInSec,
				Name:           versionName,
				Parameters:     versionParameters,
				PipelineId:     versionPipelineId,
				CodeSource:     model.CodeSource{RepoName: versionRepoName, CommitSHA: versionCommitSha},
				Status:         versionStatus,
			}})
	}
	return pipelines, nil
}

func (s *PipelineStore) scanVersionRows(rows *sql.Rows) ([]*model.PipelineVersion, error) {
	var versions []*model.PipelineVersion
	for rows.Next() {
		var uuid, name, parameters, pipelineId, repoName, commitSha string
		var createdAtInSec int64
		var status model.PipelineVersionStatus
		if err := rows.Scan(&uuid, &createdAtInSec, &name, &parameters, &pipelineId, &repoName,
			&commitSha, &status); err != nil {
			return nil, err
		}
		versions = append(versions, &model.PipelineVersion{
			UUID:           uuid,
			CreatedAtInSec: createdAtInSec,
			Name:           name,
			Parameters:     parameters,
			PipelineId:     pipelineId,
			CodeSource: model.CodeSource{
				RepoName:  repoName,
				CommitSHA: commitSha,
			},
			Status: status})
	}
	return versions, nil
}

func (s *PipelineStore) GetPipeline(id string) (*model.Pipeline, error) {
	return s.GetPipelineWithStatus(id, model.PipelineReady)
}

func (s *PipelineStore) GetPipelineWithStatus(id string, status model.PipelineStatus) (*model.Pipeline, error) {
	sql, args, err := sq.
		Select("*").
		From("pipelines").
		Where(sq.Eq{"uuid": id}).
		Where(sq.Eq{"status": status}).
		Limit(1).
		LeftJoin("pipeline_versions on pipelines.DefaultVersionId=pipeline_versions.UUID").
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to get pipeline: %v", err.Error())
	}
	r, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to get pipeline: %v", err.Error())
	}
	defer r.Close()
	pipelines, err := s.scanPipelineRows(r)

	if err != nil || len(pipelines) > 1 {
		return nil, util.NewInternalServerError(err, "Failed to get pipeline: %v", err.Error())
	}
	if len(pipelines) == 0 {
		return nil, util.NewResourceNotFoundError("Pipeline", fmt.Sprint(id))
	}
	return pipelines[0], nil
}

func (s *PipelineStore) DeletePipeline(id string) error {
	// TODO cascade delete the versions.
	sql, args, err := sq.Delete("pipelines").Where(sq.Eq{"UUID": id}).ToSql()
	if err != nil {
		return util.NewInternalServerError(err, "Failed to create query to delete pipeline: %v", err.Error())
	}

	_, err = s.db.Exec(sql, args...)
	if err != nil {
		return util.NewInternalServerError(err, "Failed to delete pipeline: %v", err.Error())
	}
	return nil
}

func (s *PipelineStore) CreatePipeline(p *model.Pipeline) (*model.Pipeline, error) {
	newPipeline := *p
	now := s.time.Now().Unix()
	newPipeline.CreatedAtInSec = now
	id, err := s.uuid.NewRandom()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a pipeline id.")
	}
	newPipeline.UUID = id.String()
	sql, args, err := sq.
		Insert("pipelines").
		SetMap(
		sq.Eq{
			"UUID":           newPipeline.UUID,
			"CreatedAtInSec": newPipeline.CreatedAtInSec,
			"Name":           newPipeline.Name,
			"Description":    newPipeline.Description,
			"Status":         string(newPipeline.Status)}).
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to insert pipeline to pipeline table: %v",
			err.Error())
	}
	_, err = s.db.Exec(sql, args...)
	if err != nil {
		if s.db.IsDuplicateError(err) {
			return nil, util.NewInvalidInputError(
				"Failed to create a new pipeline. The name %v already exist. Please specify a new name.", p.Name)
		}
		return nil, util.NewInternalServerError(err, "Failed to add pipeline to pipeline table: %v",
			err.Error())
	}
	return &newPipeline, nil
}

func (s *PipelineStore) UpdatePipelineStatus(id string, status model.PipelineStatus) error {
	sql, args, err := sq.
		Update("pipelines").
		SetMap(sq.Eq{"Status": status}).
		Where(sq.Eq{"UUID": id}).
		ToSql()
	if err != nil {
		return util.NewInternalServerError(err, "Failed to create query to update the pipeline metadata: %s", err.Error())
	}
	_, err = s.db.Exec(sql, args...)
	if err != nil {
		return util.NewInternalServerError(err, "Failed to update the pipeline metadata: %s", err.Error())
	}
	return nil
}

func (s *PipelineStore) CreatePipelineVersion(v *model.PipelineVersion) (*model.PipelineVersion, error) {
	newPipelineVersion := *v
	newPipelineVersion.CreatedAtInSec = s.time.Now().Unix()
	id, err := s.uuid.NewRandom()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a pipeline version id.")
	}
	newPipelineVersion.UUID = id.String()
	sql, args, err := sq.
		Insert("pipeline_versions").
		SetMap(
		sq.Eq{
			"UUID":           newPipelineVersion.UUID,
			"CreatedAtInSec": newPipelineVersion.CreatedAtInSec,
			"Name":           newPipelineVersion.Name,
			"Parameters":     newPipelineVersion.Parameters,
			"PipelineId":     newPipelineVersion.PipelineId,
			"RepoName":       newPipelineVersion.CodeSource.RepoName,
			"CommitSHA":      newPipelineVersion.CodeSource.CommitSHA,
			"Status":         string(newPipelineVersion.Status)}).
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to insert version to pipeline version table: %v",
			err.Error())
	}
	_, err = s.db.Exec(sql, args...)
	if err != nil {
		if s.db.IsDuplicateError(err) {
			return nil, util.NewInvalidInputError(
				"Failed to create a new pipeline version. The name %v already exist. Please specify a new name.", v.Name)
		}
		return nil, util.NewInternalServerError(err, "Failed to add version to pipeline version table: %v",
			err.Error())
	}
	return &newPipelineVersion, nil
}

func (s *PipelineStore) GetPipelineVersion(pipelineId string, versionId string) (*model.PipelineVersion, error) {
	return s.GetPipelineVersionWithStatus(pipelineId, versionId, model.PipelineVersionReady)
}

func (s *PipelineStore) GetPipelineVersionWithStatus(pipelineId string, versionId string, status model.PipelineVersionStatus) (*model.PipelineVersion, error) {
	sql, args, err := sq.
		Select("*").
		From("pipeline_versions").
		Where(sq.Eq{"uuid": versionId}).
		Where(sq.Eq{"pipelineid": pipelineId}).
		Where(sq.Eq{"status": status}).
		Limit(1).
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to get pipeline version: %v", err.Error())
	}
	r, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to get pipeline version: %v", err.Error())
	}
	defer r.Close()
	versions, err := s.scanVersionRows(r)

	if err != nil || len(versions) > 1 {
		return nil, util.NewInternalServerError(err, "Failed to get pipeline version: %v", err.Error())
	}
	if len(versions) == 0 {
		return nil, util.NewResourceNotFoundError("Version", fmt.Sprint(versionId))
	}
	return versions[0], nil
}

func (s *PipelineStore) ListPipelineVersions(pipelineId string) ([]*model.PipelineVersion, error) {
	sql, args, err := sq.
		Select("*").
		From("pipeline_versions").
		Where(sq.Eq{"pipelineid": pipelineId}).
		Where(sq.Eq{"status": model.PipelineVersionReady}).
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to list pipeline version: %v", err.Error())
	}
	r, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to list pipeline version: %v", err.Error())
	}
	defer r.Close()
	versions, err := s.scanVersionRows(r)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to list pipeline version: %v", err.Error())
	}
	return versions, nil
}

func (s *PipelineStore) UpdatePipelineVersionStatus(id string, status model.PipelineVersionStatus) error {
	sql, args, err := sq.
		Update("pipeline_versions").
		SetMap(sq.Eq{"Status": status}).
		Where(sq.Eq{"UUID": id}).
		ToSql()
	if err != nil {
		return util.NewInternalServerError(err, "Failed to create query to update the pipeline version metadata: %s", err.Error())
	}
	_, err = s.db.Exec(sql, args...)
	if err != nil {
		return util.NewInternalServerError(err, "Failed to update the pipeline version metadata: %s", err.Error())
	}
	return nil
}

// factory function for pipeline store
func NewPipelineStore(db *DB, time util.TimeInterface, uuid util.UUIDGeneratorInterface) *PipelineStore {
	return &PipelineStore{db: db, time: time, uuid: uuid}
}
