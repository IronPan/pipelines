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

package server

import (
	"context"
	"net/http"
	"net/url"
	"path"

	"github.com/golang/protobuf/ptypes/empty"
	api "github.com/kubeflow/pipelines/backend/api/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
	"github.com/kubeflow/pipelines/backend/src/common/util"
)

type PipelineServer struct {
	resourceManager *resource.ResourceManager
	httpClient      *http.Client
}

func (s *PipelineServer) CreatePipelineVersion(ctx context.Context, request *api.CreatePipelineVersionRequest) (*api.PipelineVersion, error) {
	pipelineUrl := request.Version.Url.PipelineUrl
	resp, err := s.httpClient.Get(pipelineUrl)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, util.NewInternalServerError(err, "Failed to download the pipeline from %v "+
				"Please double check the URL is valid and can be accessed by the pipeline system.", pipelineUrl)
	}
	pipelineFileName := path.Base(pipelineUrl)
	pipelineFile, err := ReadPipelineFile(pipelineFileName, resp.Body, MaxFileLength)
	if err != nil {
		return nil, util.Wrap(err, "The URL is valid but pipeline system failed to read the file.")
	}

	version, err := s.resourceManager.CreatePipelineVersion(request.Version, request.PipelineId, pipelineFile)
	if err != nil {
		return nil, util.Wrap(err, "Failed to create a version.")
	}
	return ToApiVersion(version)
}

func (s *PipelineServer) GetPipelineVersion(ctx context.Context, request *api.GetPipelineVersionRequest) (*api.PipelineVersion, error) {
	version, err := s.resourceManager.GetPipelineVersion(request.PipelineId, request.VersionId)
	if err != nil {
		return nil, util.Wrap(err, "Get pipeline version failed.")
	}
	return ToApiVersion(version)
}

func (s *PipelineServer) ListPipelineVersions(ctx context.Context, request *api.ListPipelineVersionsRequest) (*api.ListPipelineVersionsResponse, error) {
	versions, err := s.resourceManager.ListPipelineVersions(request.PipelineId)

	if err != nil {
		return nil, util.Wrap(err, "Failed to list pipeline versions")
	}
	apiVersions, err := ToApiVersions(versions)
	if err != nil {
		return nil, util.Wrap(err, "Failed to list pipeline versions")
	}
	return &api.ListPipelineVersionsResponse{Versions: apiVersions}, nil
}

func (s *PipelineServer) GetPipelineVersionTemplate(ctx context.Context, request *api.GetPipelineVersionTemplateRequest) (*api.GetTemplateResponse, error) {
	template, err := s.resourceManager.GetPipelineVersionTemplate(request.VersionId)
	if err != nil {
		return nil, util.Wrap(err, "Get pipeline version template failed.")
	}

	return &api.GetTemplateResponse{Template: string(template)}, nil
}

func (s *PipelineServer) CreatePipeline(ctx context.Context, request *api.CreatePipelineRequest) (*api.Pipeline, error) {
	pipeline, err := s.resourceManager.CreatePipeline2(request.Pipeline.Name, request.Pipeline.Description)
	if err != nil {
		return nil, util.Wrap(err, "Create pipeline failed.")
	}

	hasValidUrl, err := HasValidUrl(request)
	if err != nil {
		return nil, util.Wrap(err, "Create pipeline failed.")
	}

	if hasValidUrl {
		pipelineUrl := request.Pipeline.Url.PipelineUrl
		resp, err := s.httpClient.Get(pipelineUrl)
		if err != nil || resp.StatusCode != http.StatusOK {
			return nil, util.NewInternalServerError(err, "Failed to download the pipeline from %v "+
					"Please double check the URL is valid and can be accessed by the pipeline system.", pipelineUrl)
		}
		pipelineFileName := path.Base(pipelineUrl)
		pipelineFile, err := ReadPipelineFile(pipelineFileName, resp.Body, MaxFileLength)
		if err != nil {
			return nil, util.Wrap(err, "The URL is valid but pipeline system failed to read the file.")
		}

		version, err := s.resourceManager.CreatePipelineVersion(request.Pipeline.DefaultVersion, pipeline.UUID, pipelineFile)
		if err != nil {
			return nil, util.Wrap(err, "Failed to create a version.")
		}
		pipeline.DefaultVersion = version
	}

	return ToApiPipeline(pipeline), nil
}

func (s *PipelineServer) GetPipeline(ctx context.Context, request *api.GetPipelineRequest) (*api.Pipeline, error) {
	pipeline, err := s.resourceManager.GetPipeline(request.Id)
	if err != nil {
		return nil, util.Wrap(err, "Get pipeline failed.")
	}
	return ToApiPipeline(pipeline), nil
}

func (s *PipelineServer) ListPipelines(ctx context.Context, request *api.ListPipelinesRequest) (*api.ListPipelinesResponse, error) {
	opts, err := validatedListOptions(&model.Pipeline{}, request.PageToken, int(request.PageSize), request.SortBy, request.Filter)

	if err != nil {
		return nil, util.Wrap(err, "Failed to create list options")
	}

	pipelines, total_size, nextPageToken, err := s.resourceManager.ListPipelines(opts)
	if err != nil {
		return nil, util.Wrap(err, "List pipelines failed.")
	}
	apiPipelines := ToApiPipelines(pipelines)
	return &api.ListPipelinesResponse{Pipelines: apiPipelines, TotalSize: int32(total_size), NextPageToken: nextPageToken}, nil
}

func (s *PipelineServer) DeletePipeline(ctx context.Context, request *api.DeletePipelineRequest) (*empty.Empty, error) {
	err := s.resourceManager.DeletePipeline(request.Id)
	if err != nil {
		return nil, util.Wrap(err, "Delete pipelines failed.")
	}

	return &empty.Empty{}, nil
}

func (s *PipelineServer) GetTemplate(ctx context.Context, request *api.GetTemplateRequest) (*api.GetTemplateResponse, error) {
	template, err := s.resourceManager.GetPipelineTemplate(request.Id)
	if err != nil {
		return nil, util.Wrap(err, "Get pipeline template failed.")
	}

	return &api.GetTemplateResponse{Template: string(template)}, nil
}

func HasValidUrl(request *api.CreatePipelineRequest) (bool, error) {
	if request.Pipeline.Url == nil || request.Pipeline.Url.PipelineUrl == "" {
		return false, nil
	}

	if _, err := url.ParseRequestURI(request.Pipeline.Url.PipelineUrl); err != nil {
		return false, util.NewInvalidInputError(
			"Invalid Pipeline URL %v. Please specify a valid URL", request.Pipeline.Url.PipelineUrl)
	}
	return true, nil
}

func NewPipelineServer(resourceManager *resource.ResourceManager) *PipelineServer {
	return &PipelineServer{resourceManager: resourceManager, httpClient: http.DefaultClient}
}
