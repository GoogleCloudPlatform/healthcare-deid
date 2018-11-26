import {HttpClientTestingModule, HttpTestingController} from '@angular/common/http/testing';
import {getTestBed, inject, TestBed} from '@angular/core/testing';

import {environment} from '../environments/environment';

import {
  DlpDemoService,
  RedactImgResponse,
  ListJobsResponse,
} from './dlp-demo.service';

describe('DlpDemoService', () => {
  let service: DlpDemoService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule(
        {imports: [HttpClientTestingModule], providers: [DlpDemoService]});
    const testbed = getTestBed();
    httpMock = testbed.get(HttpTestingController);
    service = testbed.get(DlpDemoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should return a RedactImgResponse observable', () => {
    const result: RedactImgResponse = {
      redactedByteStream: 'redactedStream',
    };
    const imageType = 'jpeg';
    const fakeImgStream = 'origSteam';

    let redactImageResponse: RedactImgResponse;
    service.redactImage(imageType, fakeImgStream).subscribe(res => {
      redactImageResponse = res;
    });

    const req = httpMock.expectOne(`${environment.server}/api/demo/image`);
    expect(req.request.method).toBe('POST');
    expect(req.cancelled).toBeFalsy();
    req.flush(result);
    expect(redactImageResponse).toBeDefined();
    expect(redactImageResponse.redactedByteStream).toEqual('redactedStream');
    httpMock.verify();
  });

  it('should return a ListJobsResponse Observable', () => {
    const result: ListJobsResponse = {
      jobs: [
        {
          id: 1,
          name: 'test',
          originalQuery: 'select * from test_table',
          deidTable: 'result',
          status: 200,
          timestamp: new Date(),
        },
      ]
    };
    let listJobsResponse: ListJobsResponse;
    service.getDeidJobs().subscribe(res => {
      listJobsResponse = res;
    });

    const req = httpMock.expectOne(
      `${environment.server}/api/deidentify`);
    expect(req.request.method).toBe('GET');
    expect(req.cancelled).toBeFalsy();
    req.flush(result);
    expect(listJobsResponse).toBeDefined();
    expect(listJobsResponse.jobs[0]).toEqual(result.jobs[0]);
    httpMock.verify();
  });
});
