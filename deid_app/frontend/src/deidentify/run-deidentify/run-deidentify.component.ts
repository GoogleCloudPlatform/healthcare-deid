import {Component, OnInit, OnDestroy, ViewChild} from '@angular/core';
import {FormControl, FormGroup, Validators} from '@angular/forms';
import {MatSnackBar} from '@angular/material';
import {Observable, Subscription} from 'rxjs';
import {finalize} from 'rxjs/operators';

import {BigQueryNewTable} from '../../common/bigquery-new-table';
import {BigQueryTable} from '../../common/bigquery-table';
import {DisplayOption} from '../../common/display-option';
import {DlpDemoService} from '../../services/dlp-demo.service';
import {DlpPipelineRequest} from '../../services/dlp-demo.service';

/**
 * Represents different parameters that the dlp pipeline accepts.
 */
enum DlpParameters {
  INPUT_QUERY = 'input_query',
  INPUT_TABLE = 'input_table',
  DEID_TABLE = 'deid_table',
  FINDINGS_TABLE = 'findings_table',
}

/**
 * This component manages a form that enables the user to run the DLP pipeline.
 * The component ensures that the data entered is valid. Once the request ends,
 * the job details can be viewed in the current jobs component.
 */
@Component({
  selector: 'app-run-deidentify',
  templateUrl: './run-deidentify.component.html',
  styleUrls: [
    './run-deidentify.component.css',
    '../deidentify.component.css',
  ],
})
export class RunDeidentifyComponent implements OnInit, OnDestroy {
  readonly dlpParameters = DlpParameters;
  private readonly subscriptions = new Subscription();

  @ViewChild('submitComponent') submitComp;
  readonly submitPlaceholder = 'Submit';

  inputOptions: DisplayOption[] = [
    {value: this.dlpParameters.INPUT_TABLE, displayString: 'BiqQuery Table'},
    {value: this.dlpParameters.INPUT_QUERY, displayString: 'BigQuery Query'},
  ];

  outputOptions: DisplayOption[] = [
    {value: this.dlpParameters.DEID_TABLE, displayString: 'BigQuery'},
  ];

  findingsOptions: DisplayOption[] = [
    {value: this.dlpParameters.FINDINGS_TABLE, displayString: 'BigQuery'},
  ];

  dlpForm = new FormGroup({
    project: new FormControl(''),
    name: new FormControl(new Date().toLocaleString(), Validators.required),
    input: new FormGroup({
      method: new FormControl('', Validators.required),
      bqTable: BigQueryTable.buildEntry(),
      query: new FormControl('', Validators.required),
    }),
    output: new FormGroup({
      method: new FormControl('', Validators.required),
      bqTable: BigQueryNewTable.buildEntry(),
    }),
    findings: new FormGroup({
      method: new FormControl('', Validators.required),
      bqTable: BigQueryNewTable.buildEntry(),
    }),
    batchSize: new FormControl(1),
  });

  constructor(
      private dlpDemoService: DlpDemoService,
      public snackBar: MatSnackBar,
  ) {}

  get jobName(): FormControl {
    return this.dlpForm.get('name') as FormControl;
  }

  get inputMethod(): FormControl {
    return this.dlpForm.get('input.method') as FormControl;
  }

  get bqTableInput(): FormGroup {
    return this.dlpForm.get('input.bqTable') as FormGroup;
  }

  get inputQuery(): FormControl {
    return this.dlpForm.get('input.query') as FormControl;
  }

  get outputMethod(): FormControl {
    return this.dlpForm.get('output.method') as FormControl;
  }

  get bqTableOutput(): FormGroup {
    return this.dlpForm.get('output.bqTable') as FormGroup;
  }

  get findingsMethod(): FormControl {
    return this.dlpForm.get('findings.method') as FormControl;
  }

  get bqTableFindings(): FormGroup {
    return this.dlpForm.get('findings.bqTable') as FormGroup;
  }

  ngOnInit() {
    /* Get the project name from the server. */
    this.subscriptions.add(this.dlpDemoService.project.subscribe(
        project => this.dlpForm.patchValue({project})));

    /* Change dlpForm validation needs based on the selected user input. */
    this.subscriptions.add(this.inputMethod.valueChanges.subscribe(method => {
      if (method === this.dlpParameters.INPUT_QUERY) {
        this.bqTableInput.disable();
        this.inputQuery.enable();
      } else if (method === this.dlpParameters.INPUT_TABLE) {
        this.bqTableInput.enable();
        this.inputQuery.disable();
      }
    }));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  private getTableName(bqTable: FormGroup): string {
    const dataset = bqTable.get('dataset').value;
    const table = bqTable.get('table').value;
    return `${dataset}.${table}`;
  }

  /**
   * If the form is valid, the onSubmit is going to turn the form into a DeidJob
   * and send a request with that job to the server. A spinner will exist as
   * long as the call is running.
   */
  private onSubmit(): void {
    const job: DlpPipelineRequest = {
      name: '',
      inputMethod: '',
      inputInfo: '',
      outputMethod: '',
      outputInfo: '',
    };
    job.name = this.jobName.value;
    job.inputMethod = this.inputMethod.value;
    if (job.inputMethod === this.dlpParameters.INPUT_TABLE) {
      job.inputInfo = this.getTableName(this.bqTableInput);
    } else if (job.inputMethod === this.dlpParameters.INPUT_QUERY) {
      job.inputInfo = this.inputQuery.value;
    }

    job.outputMethod = this.outputMethod.value;
    if (job.outputMethod === this.dlpParameters.DEID_TABLE) {
      job.outputInfo = this.getTableName(this.bqTableOutput);
    }

    job.findingsTable = this.getTableName(this.bqTableFindings);
    job.batchSize = this.dlpForm.get('batchSize').value;

    this.jobName.setValue(new Date().toLocaleString());
    this.submitComp.waiting = true;
    this.dlpDemoService.deidentifyData(job)
        .pipe(finalize(() => this.submitComp.waiting = false))
        .subscribe(res => {
          let message: string;
          if (res.result === 'success') {
            message = 'Request Succeeded!';
          } else {
            message = 'Request Failed';
          }
          this.snackBar.open(message, 'Dismiss', {
            duration: 3000,
          });
          this.dlpDemoService.refreshDeidJobs();
        });
  }
}
