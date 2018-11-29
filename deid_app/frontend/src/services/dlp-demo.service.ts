import {HttpClient, HttpHeaders, HttpParams} from '@angular/common/http';
import {Injectable} from '@angular/core';
import {BehaviorSubject, Observable} from 'rxjs';
import {map} from 'rxjs/operators';

import {DeidJob} from '../deidentify/deid_job';
import {EvalJob, EvalStats} from '../evaluate/eval_job';

/**
 * Represents a successful response payload from the demo/image endpoint.
 */
export interface RedactImgResponse {
  redactedByteStream: string;
}

/**
 * Represents a successful response payload from the datasets/ endpoint.
 */
export interface BqDatasetResponse {
  datasets: string[];
}

/**
 * Represents a successful response payload from the datasets/<id>/table
 * endpoint.
 */
export interface BqTablesResponse {
  dataset: string;
  tables: string[];
}

/**
 * Represents a successful response payload from the project/ endpoint.
 */
export interface ProjectResponse {
  project: string;
}

/**
 * Represents a successful response payload from the deidentify/ endpoint.
 */
export interface DeidentifyResponse {
  result: string;
}

/**
 * An enum of the highlighting states a TextVisualizerData object can have.
 */
export enum Annotation {
  HIGHLIGHTED = 'HIGHLIGHTED',
  UNHIGHLIGHTED = 'UNHIGHLIGHTED'
}

/**
 * Represents a chunk of a note to be displayed in the Dlp Compare Data
 * component.
 */
export interface TextVisualizerData {
  annotation: Annotation;
  quote: string;
  replacement: string;
  begin: number;
  length: number;
  color: string;
}

/**
 * Represents a successful response payload from the deidentify/<>/note/<>
 * endpoint.
 */
export interface NoteHighlightsResponse {
  data: TextVisualizerData[];
}

/**
 * Represents a successful response payload from the deidentify/ GET endpoint.
 */
export interface ListJobsResponse {
  jobs: DeidJob[];
}

/**
 * Represents a request to be sent to run the DLP pipeline.
 */
export interface DlpPipelineRequest {
  name: string;
  inputMethod: string;
  inputInfo: string;
  outputMethod: string;
  outputInfo: string;
  findingsTable?: string;
  maeTable?: string;
  maeDir?: string;
  batchSize?: number;
}

/**
 * Represents the metadata schema for a note.
 */
export interface NoteMetaData {
  patientId: string;
  recordNumber: number;
}

/**
 * Represents a successful response for the deidentify/<>/metadata.
 */
export interface NotesMetadataResponse {
  notesMetadata: NoteMetaData[];
}

/**
 * Represents a successful response for the eval/stats/<jobId>
 */
export interface EvalStatsResponse {
  stats: EvalStats[];
}

/**
 * Represents gcs input requirements for the evaluation pipeline.
 */
export interface EvalGcsInput {
  gcs: {pattern: string; golden: string;};
}

/**
 * Represents BigQuery input requirements for the evaluation pipeline.
 */
export interface EvalBqInput {
  bigquery: {query: string; golden: string;};
}


/**
 * Represents gcs output requirements for the evaluation pipeline.
 */
export interface EvalGcsOutput {
  gcs: {dir: string; debug: boolean;};
}

/**
 * Represents BigQuery output requirements for the evaluation pipeline.
 */
export interface EvalBqOutput {
  bigquery: {stats: string; debug: string;};
}

/**
 * Represents a request payload for the evaluation pipeline endpoint.
 */
export interface EvalPipelineRequest {
  name: string;
  input: EvalGcsInput|EvalBqInput;
  output: EvalGcsOutput|EvalBqOutput;
}

/**
 * Represents a response for the evaluation pipeline endpoint.
 */
export interface EvalPipelineResponse {
  result: string;
}

/**
 * Represents a response for the list of Eval jobs endpoint.
 */
export interface ListEvalJobsResponse {
  jobs: EvalJob[];
  offset: number;
}

/**
 * Represents a response for the upload csv endpoint.
 */
export interface UploadCsvResponse {
  result: string;
}

/**
 * Handles interactions with the Data Loss Prevention (DLP) service for the
 * purpose of de-identifying data.
 */
@Injectable({providedIn: 'root'})
export class DlpDemoService {
  private deidJobsSource = new BehaviorSubject<DeidJob[]>([]);
  private evalJobsSource = new BehaviorSubject<EvalJob[]>([]);
  private datasetsSource = new BehaviorSubject<string[]>([]);
  private projectSource = new BehaviorSubject<string>('');
  readonly deidJobs = this.deidJobsSource.asObservable();
  readonly evalJobs = this.evalJobsSource.asObservable();
  readonly datasets = this.datasetsSource.asObservable();
  readonly project = this.projectSource.asObservable();

  constructor(private http: HttpClient) {}

  /**
   * POSTs a request to redact an image.
   * @param imgType the MIME type of the provided image to be redacted.
   * @param byteStream the base64 encoding of the provided image.
   */
  redactImage(imgType: string, byteStream: string):
      Observable<RedactImgResponse> {
    const options = {
      headers: new HttpHeaders().set('Content-Type', 'application/json'),
    };

    /** Remove image metadata from the base64 encoding of the byteStream. */
    const data = {
      'type': imgType,
      'data': byteStream.split(',')[1],
    };

    return this.http.post<RedactImgResponse>(`/api/demo/image`, data, options);
  }

  /**
   * GETs project name from the server.
   */
  refreshProject(): void {
    this.http.get<ProjectResponse>(`/api/project`)
        .subscribe(
            (project: ProjectResponse) =>
                this.projectSource.next(project.project));
  }

  /**
   * GETs all project's BigQuery datasets from server.
   */
  refreshDatasets(): void {
    this.http.get<BqDatasetResponse>(`/api/datasets`)
        .subscribe(
            (datasets: BqDatasetResponse) =>
                this.datasetsSource.next(datasets.datasets));
  }

  /**
   * GETs all the tables that exist within a BigQuery dataset.
   * @param datasetId the dataset's identifier.
   */
  getTables(datasetId: string): Observable<BqTablesResponse> {
    return this.http.get<BqTablesResponse>(`/api/datasets/${datasetId}/tables`);
  }

  /**
   * POSTs a request to deidentify a BQ table/query and store the results in a
   * BigQuery table.
   * @param job the request payload with all job parameters.
   */
  deidentifyData(job: DlpPipelineRequest): Observable<DeidentifyResponse> {
    const options = {
      headers: new HttpHeaders().set('Content-Type', 'application/json'),
    };

    return this.http.post<DeidentifyResponse>(`/api/deidentify`, job, options);
  }

  /**
   * GETs all the deidentify jobs that were run previously. The results are
   * stored in the deidJobsSource subject.
   */
  refreshDeidJobs(): void {
    this.http.get<ListJobsResponse>(`/api/deidentify`)
        .subscribe((list: ListJobsResponse) => {
          this.deidJobsSource.next(list.jobs);
        });
  }

  /**
   * GETs the record numbers and patient Ids for a specified job id.
   */
  getNotesMetadata(deidJobId: number): Observable<NoteMetaData[]> {
    return this.http
        .get<NotesMetadataResponse>(`/api/deidentify/${deidJobId}/metadata`)
        .pipe(map(res => res.notesMetadata));
  }

  /**
   * GETs a list of chunks that make up a note along with the highlighting
   * information.
   */
  getNoteHighlights(deidJobId: number, recordNumber: number):
      Observable<TextVisualizerData[]> {
    return this.http
        .get<NoteHighlightsResponse>(
            `/api/deidentify/${deidJobId}/note/${recordNumber}`)
        .pipe(map(res => res.data));
  }

  /**
   * GETs all evaluation jobs that were run previouslt. The results are stored
   * in the evalJobsSource subject.
   */
  refreshEvalJobs(): void {
    this.http.get<ListEvalJobsResponse>(`/api/eval`)
        .subscribe(list => this.evalJobsSource.next(list.jobs));
  }

  /**
   * GETs the statistics results for a specific jobId.
   */
  getEvalStats(jobId: number): Observable<EvalStats[]> {
    return this.http.get<EvalStatsResponse>(`/api/eval/stats/${jobId}`)
        .pipe(map(response => response.stats));
  }

  /**
   * POSTs a request to evaluate a findings and store the results in GCS or
   * BigQuery.
   * @param job the request payload with all job parameters.
   */
  evaluateFindings(job: EvalPipelineRequest): Observable<EvalPipelineResponse> {
    const options = {
      headers: new HttpHeaders().set('Content-Type', 'application/json'),
    };

    return this.http.post<EvalPipelineResponse>(`/api/eval`, job, options);
  }

  /**
   * POSTs a request to upload a table to be used in the Dlp pipeline.
   */
  uploadCsv(csvInfo: FormData): Observable<string> {
    return this.http
        .post<UploadCsvResponse>(`/api/deidentify/upload/table`, csvInfo)
        .pipe(map(response => response.result));
  }
}
